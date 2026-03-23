using FileStorage.Abstractions;
using FileStorage.Abstractions.SecondaryIndex;
using FileStorage.Infrastructure.Checkpoint;
using FileStorage.Infrastructure.Compaction;
using FileStorage.Infrastructure.Concurrency;
using FileStorage.Infrastructure.Indexing;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Indexing.SecondaryIndex;
using FileStorage.Infrastructure.IO;
using FileStorage.Infrastructure.Recovery;
using FileStorage.Infrastructure.Serialization;
using FileStorage.Infrastructure.WAL;
using System.Buffers;

namespace FileStorage.Infrastructure;

/// <summary>
/// Storage engine facade. Coordinates locking and delegates to
/// <see cref="IIndexManager"/> (writes), <see cref="IRecordReader"/> (reads),
/// <see cref="ICompactionService"/> (maintenance), and
/// <see cref="ISecondaryIndexManager"/> (secondary indexes).
/// Contains no serialization, position tracking, or business logic.
/// </summary>
internal sealed class StorageEngine : IStorageEngine, IDisposable
{
    private readonly RegionProvider _regions;
    private readonly IWriteAheadLog _wal;
    private readonly IMemoryIndex _memoryIndex;
    private readonly IIndexManager _indexManager;
    private readonly IRecordReader _recordReader;
    private readonly IStorageRecovery _recovery;
    private readonly ICompactionService _compaction;
    private readonly ISecondaryIndexManager _secondaryIndex;
    private readonly AsyncReaderWriterLock _lock = new();
    private readonly FileLock? _fileLock;

    /// <summary>
    /// Degree of parallelism for bulk reads on NVMe storage.
    /// </summary>
    private const int ReadParallelism = 4;

    private ICheckpointManager _checkpoint;
    private bool _disposed;

    internal StorageEngine(
        RegionProvider regions,
        IWriteAheadLog wal,
        IMemoryIndex? memoryIndex = null,
        IIndexManager? indexManager = null,
        IRecordReader? recordReader = null,
        ICheckpointManager? checkpoint = null,
        IStorageRecovery? recovery = null,
        ICompactionService? compaction = null,
        ISecondaryIndexManager? secondaryIndex = null,
        FileLock? fileLock = null)
    {
        _regions = regions;
        _wal = wal;
        _memoryIndex = memoryIndex ?? new MemoryIndex();
        _indexManager = indexManager ?? new IndexManager(regions, _memoryIndex);
        _recordReader = recordReader ?? new RecordReader();
        _checkpoint = checkpoint ?? new CheckpointManager(regions.IndexRegion, regions.DataRegion, wal);
        _recovery = recovery ?? new StorageRecovery();
        _compaction = compaction ?? new CompactionService();
        _secondaryIndex = secondaryIndex ?? new SecondaryIndexManager(
            Path.GetDirectoryName(regions.IndexRegion.Path) ?? ".");
        _fileLock = fileLock;
    }

    // ──────────────────────────────────────────────
    //  Write operations (exclusive lock)
    // ──────────────────────────────────────────────

    public async Task InitializeAsync()
    {
        using var _ = await _lock.AcquireWriteLockAsync();

        var result = _recovery.Initialize(
            _regions.IndexRegion, _regions.DataRegion, _wal, _memoryIndex, _indexManager);
        _indexManager.SetWritePositions(result.IndexWritePos, result.DataWritePos);

        if (_secondaryIndex is SecondaryIndexManager sim)
            sim.LoadExisting();

        // Replay secondary index entries from WAL
        ReplaySecondaryIndexes();
    }

    public async Task SaveAsync(string table, Guid key, byte[] data)
    {
        await SaveAsync(table, key, data, new Dictionary<string, string>());
    }

    /// <summary>
    /// Saves data with indexed field values.
    /// Both primary index and secondary indexes are updated atomically:
    /// indexed fields are persisted in the WAL entry, so they survive crash recovery.
    /// </summary>
    public async Task SaveAsync(string table, Guid key, byte[] data, IReadOnlyDictionary<string, string> indexedFields)
    {
        if (string.IsNullOrEmpty(table)) throw new ArgumentException("Table name required", nameof(table));
        ArgumentNullException.ThrowIfNull(data);
        if (data.Length == 0) throw new ArgumentException("Data cannot be empty", nameof(data));

        _indexManager.ValidateTableName(table);

        using var _ = await _lock.AcquireWriteLockAsync();

        var (dataOffset, indexOffset) = _indexManager.ApplySave(table, key, data);

        // WAL entry includes indexed fields — if we crash after WAL.Append but before
        // _secondaryIndex.Put, the fields are replayed from WAL during recovery.
        _wal.Append(new WalEntry
        {
            Operation = WalOperationType.Save,
            Table = table, Key = key, Data = data,
            DataOffset = dataOffset, IndexOffset = indexOffset,
            IndexedFields = indexedFields
        });

        if (indexedFields.Count > 0)
            _secondaryIndex.Put(table, key, indexedFields);

        _checkpoint.TrackWrite();
    }

    /// <summary>
    /// Deletes a record and removes it from all secondary indexes.
    /// Uses <see cref="ISecondaryIndexManager.RemoveByKey"/> since the previously
    /// indexed values are not known at delete time.
    /// </summary>
    public async Task DeleteAsync(string table, Guid key)
    {
        using var _ = await _lock.AcquireWriteLockAsync();

        if (!_memoryIndex.TryGet(table, key, out long indexOffset)) return;

        _wal.Append(new WalEntry
        {
            Operation = WalOperationType.Delete,
            Table = table, Key = key, Data = [],
            DataOffset = 0, IndexOffset = indexOffset,
            IndexedFields = new Dictionary<string, string>()
        });

        _indexManager.ApplyDelete(table, key, indexOffset);
        _secondaryIndex.RemoveByKey(table, key);
        _checkpoint.TrackWrite();
    }

    /// <summary>
    /// Drops an entire table with a single WAL record.
    /// Physical cleanup of index entries happens immediately in-memory;
    /// disk space is reclaimed by <see cref="CompactAsync"/>.
    /// </summary>
    public async Task<long> DropTableAsync(string table)
    {
        if (string.IsNullOrEmpty(table))
            throw new ArgumentException("Table name required", nameof(table));

        using var _ = await _lock.AcquireWriteLockAsync();

        long count = _memoryIndex.CountByTable(table);
        if (count == 0) return 0;

        // Single WAL record for the entire table drop — O(1) disk IO.
        _wal.Append(new WalEntry
        {
            Operation = WalOperationType.DropTable,
            Table = table, Key = Guid.Empty, Data = [],
            DataOffset = 0, IndexOffset = 0,
            IndexedFields = new Dictionary<string, string>()
        });

        _indexManager.ApplyDropTable(table);
        _secondaryIndex.DropAllIndexes(table);
        _checkpoint.TrackWrite();

        return count;
    }

    /// <summary>
    /// Removes all records from a table. The table remains queryable (returns empty).
    /// A single WAL record marks all data as invalid for crash recovery.
    /// Disk space is reclaimed by <see cref="CompactAsync"/>.
    /// </summary>
    public async Task<long> TruncateTableAsync(string table)
    {
        if (string.IsNullOrEmpty(table))
            throw new ArgumentException("Table name required", nameof(table));

        using var _ = await _lock.AcquireWriteLockAsync();

        long count = _memoryIndex.CountByTable(table);
        if (count == 0) return 0;

        _wal.Append(new WalEntry
        {
            Operation = WalOperationType.TruncateTable,
            Table = table, Key = Guid.Empty, Data = [],
            DataOffset = 0, IndexOffset = 0,
            IndexedFields = new Dictionary<string, string>()
        });

        _indexManager.ApplyTruncateTable(table);
        _secondaryIndex.DropAllIndexes(table);
        _checkpoint.TrackWrite();

        return count;
    }

    public async Task<long> CompactAsync(params string[] tables)
    {
        using var _ = await _lock.AcquireWriteLockAsync();

        _checkpoint.ForceCheckpoint();

        IReadOnlySet<string>? scope = tables.Length > 0
            ? new HashSet<string>(tables, StringComparer.Ordinal)
            : null;

        long removed = _compaction.Compact(
            _regions.IndexRegion, _regions.DataRegion, _memoryIndex,
            reopenRegion: _regions.Reopen,
            scope);

        _indexManager.RecalculateWritePositions();

        _checkpoint = new CheckpointManager(_regions.IndexRegion, _regions.DataRegion, _wal);

        return removed;
    }

    // ──────────────────────────────────────────────
    //  Secondary index operations
    // ──────────────────────────────────────────────
    public async Task DropIndexAsync(string table, string fieldName)
    {
        using var _ = await _lock.AcquireWriteLockAsync();
        _secondaryIndex.DropIndex(table, fieldName);
    }

    public async Task<IReadOnlyList<IndexDefinition>> GetIndexesAsync(string table)
    {
        using var _ = await _lock.AcquireReadLockAsync();
        return _secondaryIndex.GetIndexes(table);
    }

    public async Task<List<Guid>?> LookupByIndexAsync(string table, string fieldName, string value)
    {
        using var _ = await _lock.AcquireReadLockAsync();
        if (!_secondaryIndex.HasIndex(table, fieldName))
            return null;
        return _secondaryIndex.Lookup(table, fieldName, value);
    }

    // ──────────────────────────────────────────────
    //  Read operations (shared lock)
    // ──────────────────────────────────────────────

    public async Task<StorageRecord?> GetByKeyAsync(string table, Guid key)
    {
        using var _ = await _lock.AcquireReadLockAsync();

        if (!_memoryIndex.TryGet(table, key, out long indexOffset)) return null;

        var buffer = ArrayPool<byte>.Shared.Rent(_indexManager.EntrySize);
        try
        {
            return _recordReader.Read(
                _regions.IndexRegion, _regions.DataRegion, buffer, indexOffset, table, key);
        }
        finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }
    }

    /// <summary>
    /// Returns records for a table with skip/take pagination.
    /// Uses parallel reads when candidate count exceeds <see cref="ReadParallelism"/>
    /// to saturate NVMe bandwidth. Each read uses its own buffer from ArrayPool.
    /// MmapRegion.Read is thread-safe under read lock (snapshot-based accessor).
    /// </summary>
    public async Task<List<StorageRecord>> GetByTableAsync(string table, int skip = 0, int take = int.MaxValue)
    {
        using var _ = await _lock.AcquireReadLockAsync();

        var candidates = _memoryIndex.GetByTable(table, skip, take);
        if (candidates.Count == 0)
            return [];

        // Small result set — sequential is faster (no Task overhead)
        if (candidates.Count <= ReadParallelism)
            return ReadSequential(candidates, table);

        // Large result set — parallel reads
        return await ReadParallelAsync(candidates, table, take);
    }

    public async Task<long> CountAsync(string table)
    {
        using var _ = await _lock.AcquireReadLockAsync();
        return _memoryIndex.CountByTable(table);
    }

    public async Task<IReadOnlyList<string>> ListTablesAsync()
    {
        using var _ = await _lock.AcquireReadLockAsync();
        return _memoryIndex.GetTableNames();
    }

    public async Task<bool> TableExistsAsync(string table)
    {
        if (string.IsNullOrEmpty(table)) return false;

        using var _ = await _lock.AcquireReadLockAsync();
        return _memoryIndex.TableExists(table);
    }

    // ──────────────────────────────────────────────
    //  Lifecycle
    // ──────────────────────────────────────────────

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        try { _checkpoint.ForceCheckpoint(); }
        catch (ObjectDisposedException) { }

        if (_secondaryIndex is IDisposable disposable)
            disposable.Dispose();

        _wal.Dispose();
        _regions.Dispose();
        _lock.Dispose();
        _fileLock?.Dispose();
    }

    // ──────────────────────────────────────────────
    //  Private helpers
    // ──────────────────────────────────────────────

    /// <summary>
    /// Replays WAL entries to rebuild secondary index state that may have been
    /// lost if a crash occurred between WAL.Append and _secondaryIndex.Put.
    /// Only processes Save entries that have non-empty IndexedFields.
    /// </summary>
    private void ReplaySecondaryIndexes()
    {
        var entries = _wal.ReadAll();
        foreach (var entry in entries)
        {
            switch (entry.Operation)
            {
                case WalOperationType.Save when entry.IndexedFields is { Count: > 0 }:
                    _secondaryIndex.Put(entry.Table, entry.Key, entry.IndexedFields);
                    break;

                case WalOperationType.Delete:
                    _secondaryIndex.RemoveByKey(entry.Table, entry.Key);
                    break;

                case WalOperationType.DropTable:
                case WalOperationType.TruncateTable:
                    _secondaryIndex.DropAllIndexes(entry.Table);
                    break;
            }
        }
    }

    private List<StorageRecord> ReadSequential(
        IReadOnlyList<(Guid Key, long Offset)> candidates, string table)
    {
        var result = new List<StorageRecord>(candidates.Count);
        var buffer = ArrayPool<byte>.Shared.Rent(_indexManager.EntrySize);
        try
        {
            foreach (var (k, indexOffset) in candidates)
            {
                var record = _recordReader.Read(
                    _regions.IndexRegion, _regions.DataRegion, buffer, indexOffset, table, k);

                if (record is not null)
                    result.Add(record);
            }
        }
        finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }

        return result;
    }

    /// <summary>
    /// Reads records in parallel batches. Each batch of <see cref="ReadParallelism"/>
    /// records is read concurrently using Task.Run + per-task ArrayPool buffer.
    /// 
    /// Thread safety: <see cref="MmapRegion.Read"/> is safe under the shared read lock
    /// because it acquires a snapshot reference — concurrent readers never see a disposed accessor.
    /// </summary>
    private async Task<List<StorageRecord>> ReadParallelAsync(
        IReadOnlyList<(Guid Key, long Offset)> candidates, string table, int take)
    {
        var result = new List<StorageRecord>(Math.Min(candidates.Count, take));
        int entrySize = _indexManager.EntrySize;

        // Process in chunks to limit concurrent tasks
        for (int i = 0; i < candidates.Count && result.Count < take; i += ReadParallelism)
        {
            int batchSize = Math.Min(ReadParallelism, candidates.Count - i);
            var tasks = new Task<StorageRecord?>[batchSize];

            for (int j = 0; j < batchSize; j++)
            {
                var (k, indexOffset) = candidates[i + j];
                tasks[j] = Task.Run(() =>
                {
                    var buf = ArrayPool<byte>.Shared.Rent(entrySize);
                    try
                    {
                        return _recordReader.Read(
                            _regions.IndexRegion, _regions.DataRegion, buf, indexOffset, table, k);
                    }
                    finally { ArrayPool<byte>.Shared.Return(buf, clearArray: true); }
                });
            }

            var records = await Task.WhenAll(tasks);

            foreach (var record in records)
            {
                if (result.Count >= take) break;
                if (record is not null)
                    result.Add(record);
            }
        }

        return result;
    }

    public async Task EnsureIndexAsync(string table, string fieldName)
    {
        if (string.IsNullOrEmpty(fieldName))
            throw new ArgumentException("Field name required", nameof(fieldName));

        using var _ = await _lock.AcquireWriteLockAsync();
        _secondaryIndex.EnsureIndex(table, fieldName);
    }
}
