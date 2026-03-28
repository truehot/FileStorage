using FileStorage.Infrastructure.Checkpoint;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Indexing.SecondaryIndex;
using FileStorage.Infrastructure.Core.Models;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure.Core.Operations;

/// <summary>
/// Executes write-side storage operations under a caller-owned write lock.
/// </summary>
internal sealed class StorageWriteOperations
{
    private readonly IWriteAheadLog _wal;
    private readonly IMemoryIndex _memoryIndex;
    private readonly IIndexManager _indexManager;
    private readonly ISecondaryIndexManager _secondaryIndex;
    private readonly CheckpointHandle _checkpointHandle;

    internal StorageWriteOperations(
        IWriteAheadLog wal,
        IMemoryIndex memoryIndex,
        IIndexManager indexManager,
        ISecondaryIndexManager secondaryIndex,
        CheckpointHandle checkpointHandle)
    {
        ArgumentNullException.ThrowIfNull(wal);
        ArgumentNullException.ThrowIfNull(memoryIndex);
        ArgumentNullException.ThrowIfNull(indexManager);
        ArgumentNullException.ThrowIfNull(secondaryIndex);
        ArgumentNullException.ThrowIfNull(checkpointHandle);

        _wal = wal;
        _memoryIndex = memoryIndex;
        _indexManager = indexManager;
        _secondaryIndex = secondaryIndex;
        _checkpointHandle = checkpointHandle;
    }

    /// <summary>
    /// Saves one raw payload without secondary indexed fields.
    /// </summary>
    public Task SaveAsync(string table, Guid key, byte[] data) =>
        SaveAsync(table, key, data, new Dictionary<string, string>());

    /// <summary>
    /// Saves data with indexed field values.
    /// </summary>
    public Task SaveAsync(string table, Guid key, byte[] data, IReadOnlyDictionary<string, string> indexedFields)
    {
        if (string.IsNullOrEmpty(table))
            throw new ArgumentException("Table name required", nameof(table));
        ArgumentNullException.ThrowIfNull(data);
        if (data.Length == 0)
            throw new ArgumentException("Data cannot be empty", nameof(data));

        _indexManager.ValidateTableName(table);

        long dataOffset = _indexManager.NextDataOffset;
        long indexOffset = _indexManager.NextIndexOffset;

        if (indexedFields.Count > 0)
        {
            byte[] payload = WalBatchPayloadSerializer.Serialize([
                new WalBatchEntry(key, data, dataOffset, indexOffset, indexedFields)
            ]);

            _wal.Append(new WalEntry
            {
                Operation = WalOperationType.SaveBatch,
                Table = table,
                Key = Guid.Empty,
                Data = payload,
                DataOffset = dataOffset,
                IndexOffset = indexOffset,
                IndexedFields = new Dictionary<string, string>()
            });
        }
        else
        {
            _wal.Append(new WalEntry
            {
                Operation = WalOperationType.Save,
                Table = table,
                Key = key,
                Data = data,
                DataOffset = dataOffset,
                IndexOffset = indexOffset,
                IndexedFields = indexedFields
            });
        }

        _indexManager.ApplySavePhysical(table, key, data, dataOffset, indexOffset);
        _indexManager.PublishSave(table, key, indexOffset);

        if (indexedFields.Count > 0)
            _secondaryIndex.Put(table, key, indexedFields);

        _checkpointHandle.Current.TrackWrite();
        return Task.CompletedTask;
    }

    /// <summary>
    /// Saves a pre-serialized batch for a single table.
    /// </summary>
    public Task SaveBatchAsync(string table, IReadOnlyCollection<StorageWriteEntry> entries)
    {
        if (string.IsNullOrEmpty(table))
            throw new ArgumentException("Table name required", nameof(table));
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
            throw new ArgumentException("Batch cannot be empty.", nameof(entries));

        _indexManager.ValidateTableName(table);

        long nextDataOffset = _indexManager.NextDataOffset;
        long nextIndexOffset = _indexManager.NextIndexOffset;

        var walEntries = new List<WalBatchEntry>(entries.Count);
        foreach (var entry in entries)
        {
            if (entry.Key == Guid.Empty)
                throw new ArgumentException("Batch item key cannot be empty.", nameof(entries));
            ArgumentNullException.ThrowIfNull(entry.Data);
            if (entry.Data.Length == 0)
                throw new ArgumentException("Batch item data cannot be empty.", nameof(entries));
            ArgumentNullException.ThrowIfNull(entry.IndexedFields);

            walEntries.Add(new WalBatchEntry(
                entry.Key,
                entry.Data,
                nextDataOffset,
                nextIndexOffset,
                entry.IndexedFields));

            nextDataOffset += entry.Data.Length;
            nextIndexOffset += _indexManager.EntrySize;
        }

        byte[] payload = WalBatchPayloadSerializer.Serialize(walEntries);

        _wal.Append(new WalEntry
        {
            Operation = WalOperationType.SaveBatch,
            Table = table,
            Key = Guid.Empty,
            Data = payload,
            DataOffset = walEntries[0].DataOffset,
            IndexOffset = walEntries[0].IndexOffset,
            IndexedFields = new Dictionary<string, string>()
        });

        foreach (var entry in walEntries)
            _indexManager.ApplySavePhysical(table, entry.Key, entry.Data, entry.DataOffset, entry.IndexOffset);

        foreach (var entry in walEntries)
            _indexManager.PublishSave(table, entry.Key, entry.IndexOffset);

        var secondaryBatch = new List<(Guid RecordKey, IReadOnlyDictionary<string, string> IndexedFields)>(walEntries.Count);
        foreach (var entry in walEntries)
            secondaryBatch.Add((entry.Key, entry.IndexedFields));

        _secondaryIndex.PutRange(table, secondaryBatch);
        _checkpointHandle.Current.TrackWrite();

        return Task.CompletedTask;
    }

    /// <summary>
    /// Deletes a record and removes it from all secondary indexes.
    /// </summary>
    public Task DeleteAsync(string table, Guid key)
    {
        if (!_memoryIndex.TryGet(table, key, out long indexOffset))
            return Task.CompletedTask;

        _wal.Append(new WalEntry
        {
            Operation = WalOperationType.Delete,
            Table = table,
            Key = key,
            Data = [],
            DataOffset = 0,
            IndexOffset = indexOffset,
            IndexedFields = new Dictionary<string, string>()
        });

        _indexManager.ApplyDelete(table, key, indexOffset);
        _secondaryIndex.RemoveByKey(table, key);
        _checkpointHandle.Current.TrackWrite();

        return Task.CompletedTask;
    }

    /// <summary>
    /// Drops all records from the specified table.
    /// </summary>
    public Task<long> DropTableAsync(string table)
    {
        if (string.IsNullOrEmpty(table))
            throw new ArgumentException("Table name required", nameof(table));

        long count = _memoryIndex.CountByTable(table);
        if (count == 0)
            return Task.FromResult(0L);

        _wal.Append(new WalEntry
        {
            Operation = WalOperationType.DropTable,
            Table = table,
            Key = Guid.Empty,
            Data = [],
            DataOffset = 0,
            IndexOffset = 0,
            IndexedFields = new Dictionary<string, string>()
        });

        _indexManager.ApplyDropTable(table);
        _secondaryIndex.DropAllIndexes(table);
        _checkpointHandle.Current.TrackWrite();

        return Task.FromResult(count);
    }

    /// <summary>
    /// Removes all records from a table while keeping the table queryable.
    /// </summary>
    public Task<long> TruncateTableAsync(string table)
    {
        if (string.IsNullOrEmpty(table))
            throw new ArgumentException("Table name required", nameof(table));

        long count = _memoryIndex.CountByTable(table);
        if (count == 0)
            return Task.FromResult(0L);

        _wal.Append(new WalEntry
        {
            Operation = WalOperationType.TruncateTable,
            Table = table,
            Key = Guid.Empty,
            Data = [],
            DataOffset = 0,
            IndexOffset = 0,
            IndexedFields = new Dictionary<string, string>()
        });

        _indexManager.ApplyTruncateTable(table);
        _secondaryIndex.DropAllIndexes(table);
        _checkpointHandle.Current.TrackWrite();

        return Task.FromResult(count);
    }

    /// <summary>
    /// Deletes a batch of records identified by their keys.
    /// </summary>
    public Task DeleteBatchAsync(string table, IEnumerable<Guid> keys)
    {
        if (string.IsNullOrEmpty(table))
            throw new ArgumentException("Table name required", nameof(table));
        ArgumentNullException.ThrowIfNull(keys);

        var keyList = keys.Where(k => k != Guid.Empty).Distinct().ToList();
        if (keyList.Count == 0)
            return Task.CompletedTask;

        // Write a single WAL entry for the batch delete
        _wal.Append(new WAL.WalEntry
        {
            Operation = WAL.WalOperationType.DeleteBatch,
            Table = table,
            Key = Guid.Empty,
            Data = WAL.WalBatchDeletePayloadSerializer.Serialize(keyList),
            DataOffset = 0,
            IndexOffset = 0,
            IndexedFields = new Dictionary<string, string>()
        });

        // Remove from indexes
        foreach (var key in keyList)
        {
            if (_memoryIndex.TryGet(table, key, out long indexOffset))
            {
                _indexManager.ApplyDelete(table, key, indexOffset);
                _secondaryIndex.RemoveByKey(table, key);
            }
        }

        _checkpointHandle.Current.TrackWrite();
        return Task.CompletedTask;
    }
}
