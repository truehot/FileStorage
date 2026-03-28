using FileStorage.Abstractions;
using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Core.Serialization;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace FileStorage.Infrastructure.Core.Operations;

/// <summary>
/// Executes read-side storage operations under a caller-owned read lock.
/// </summary>
internal sealed class StorageReadOperations
{
    /// <summary>
    /// Degree of parallelism for bulk reads on NVMe storage.
    /// </summary>
    private readonly int _readParallelism = 4;

    private readonly IRegionProvider _regions;
    private readonly IMemoryIndex _memoryIndex;
    private readonly IIndexManager _indexManager;
    private readonly IRecordReader _recordReader;

    internal StorageReadOperations(
        IRegionProvider regions,
        IMemoryIndex memoryIndex,
        IIndexManager indexManager,
        IRecordReader recordReader,
        int readParallelism = 4)
    {
        ArgumentNullException.ThrowIfNull(regions);
        ArgumentNullException.ThrowIfNull(memoryIndex);
        ArgumentNullException.ThrowIfNull(indexManager);
        ArgumentNullException.ThrowIfNull(recordReader);

        _regions = regions;
        _memoryIndex = memoryIndex;
        _indexManager = indexManager;
        _recordReader = recordReader;
        _readParallelism = readParallelism > 0 ? readParallelism : 4;
    }

    /// <summary>
    /// Reads one record by primary key.
    /// </summary>
    public Task<StorageRecord?> GetByKeyAsync(string table, Guid key)
    {
        if (!_memoryIndex.TryGet(table, key, out long indexOffset))
            return Task.FromResult<StorageRecord?>(null);

        var buffer = ArrayPool<byte>.Shared.Rent(_indexManager.EntrySize);
        try
        {
            var record = _recordReader.Read(
                _regions.IndexRegion, _regions.DataRegion, buffer, indexOffset, table, key);

            return Task.FromResult(record);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    /// <summary>
    /// Returns records for a table with skip/take pagination.
    /// </summary>
    public Task<List<StorageRecord>> GetByTableAsync(
        string table,
        int skip = 0,
        int take = int.MaxValue,
        CancellationToken cancellationToken = default)
    {
        var candidates = _memoryIndex.GetByTable(table, skip, take);
        if (candidates.Count == 0)
            return Task.FromResult(new List<StorageRecord>());

        if (candidates.Count <= _readParallelism)
            return Task.FromResult(ReadSequential(candidates, table));

        return ReadParallelAsync(candidates, table, take, cancellationToken);
    }

    /// <summary>
    /// Streams records for a table lazily.
    /// </summary>
    public async IAsyncEnumerable<StorageRecord> GetByTableStreamAsync(
        string table,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.CompletedTask.ConfigureAwait(false);

        var candidates = _memoryIndex.GetByTable(table, skip: 0, take: int.MaxValue);
        if (candidates.Count == 0)
            yield break;

        var buffer = ArrayPool<byte>.Shared.Rent(_indexManager.EntrySize);
        try
        {
            foreach (var (key, indexOffset) in candidates)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var record = _recordReader.Read(
                    _regions.IndexRegion, _regions.DataRegion, buffer, indexOffset, table, key);

                if (record is not null)
                    yield return record;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }
    }

    /// <summary>
    /// Returns the number of live records in the specified table.
    /// </summary>
    public Task<long> CountAsync(string table) =>
        Task.FromResult(_memoryIndex.CountByTable(table));

    /// <summary>
    /// Returns names of tables that currently contain live records.
    /// </summary>
    public Task<IReadOnlyList<string>> ListTablesAsync() =>
        Task.FromResult(_memoryIndex.GetTableNames());

    /// <summary>
    /// Returns <c>true</c> if a table currently exists and contains live records.
    /// </summary>
    public Task<bool> TableExistsAsync(string table)
    {
        if (string.IsNullOrEmpty(table))
            return Task.FromResult(false);

        return Task.FromResult(_memoryIndex.TableExists(table));
    }

    /// <summary>
    /// Returns records by a list of primary keys with skip/take pagination.
    /// </summary>
    public Task<List<StorageRecord>> GetByKeysAsync(
        string table,
        IReadOnlyList<Guid> keys,
        int skip = 0,
        int take = int.MaxValue,
        CancellationToken cancellationToken = default)
    {
        if (skip < 0)
            throw new ArgumentOutOfRangeException(nameof(skip), "Skip must be non-negative.");
        if (take < 0)
            throw new ArgumentOutOfRangeException(nameof(take), "Take must be non-negative.");
        if (take == 0 || keys.Count == 0)
            return Task.FromResult(new List<StorageRecord>());

        var result = new List<StorageRecord>(Math.Min(keys.Count, take));
        int skipped = 0;

        var buffer = ArrayPool<byte>.Shared.Rent(_indexManager.EntrySize);
        try
        {
            foreach (var key in keys)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (result.Count >= take)
                    break;

                if (!_memoryIndex.TryGet(table, key, out long indexOffset))
                    continue;

                var record = _recordReader.Read(
                    _regions.IndexRegion, _regions.DataRegion, buffer, indexOffset, table, key);

                if (record is null)
                    continue;

                if (skipped < skip)
                {
                    skipped++;
                    continue;
                }

                result.Add(record);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }

        return Task.FromResult(result);
    }

    private List<StorageRecord> ReadSequential(IReadOnlyList<(Guid Key, long Offset)> candidates, string table)
    {
        var result = new List<StorageRecord>(candidates.Count);
        var buffer = ArrayPool<byte>.Shared.Rent(_indexManager.EntrySize);
        try
        {
            foreach (var (key, indexOffset) in candidates)
            {
                var record = _recordReader.Read(
                    _regions.IndexRegion, _regions.DataRegion, buffer, indexOffset, table, key);

                if (record is not null)
                    result.Add(record);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
        }

        return result;
    }

    /// <summary>
    /// Reads records in parallel batches while keeping the caller-owned read lock.
    /// </summary>
    private async Task<List<StorageRecord>> ReadParallelAsync(
        IReadOnlyList<(Guid Key, long Offset)> candidates,
        string table,
        int take,
        CancellationToken cancellationToken)
    {
        var result = new List<StorageRecord>(Math.Min(candidates.Count, take));
        int entrySize = _indexManager.EntrySize;

        for (int i = 0; i < candidates.Count && result.Count < take; i += _readParallelism)
        {
            int batchSize = Math.Min(_readParallelism, candidates.Count - i);
            var tasks = new Task<StorageRecord?>[batchSize];

            for (int j = 0; j < batchSize; j++)
            {
                var (key, indexOffset) = candidates[i + j];
                tasks[j] = Task.Run(() =>
                {
                    var buffer = ArrayPool<byte>.Shared.Rent(entrySize);
                    try
                    {
                        return _recordReader.Read(
                            _regions.IndexRegion, _regions.DataRegion, buffer, indexOffset, table, key);
                    }
                    finally
                    {
                        ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
                    }
                }, cancellationToken);
            }

            var records = await Task.WhenAll(tasks).ConfigureAwait(false);

            foreach (var record in records)
            {
                if (result.Count >= take)
                    break;

                if (record is not null)
                    result.Add(record);
            }
        }

        return result;
    }
}
