using FileStorage.Abstractions;
using FileStorage.Abstractions.SecondaryIndex;
using FileStorage.Infrastructure.Checkpoint;
using FileStorage.Infrastructure.Core.Concurrency;
using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Core.Lifecycle;
using FileStorage.Infrastructure.Core.Models;
using FileStorage.Infrastructure.Core.Operations;
using FileStorage.Infrastructure.Indexing.SecondaryIndex;
using FileStorage.Infrastructure.WAL;
using System.Runtime.CompilerServices;

namespace FileStorage.Infrastructure;

/// <summary>
/// Storage engine facade. Coordinates locking, lifetime, and delegated operation services.
/// </summary>
internal sealed class StorageEngine : IStorageEngine, IDisposable
{
    private readonly IRegionProvider _regions;
    private readonly IWriteAheadLog _wal;
    private readonly ISecondaryIndexManager _secondaryIndex;
    private readonly CheckpointHandle _checkpointHandle;
    private readonly StorageEngineLifetime _lifetime;
    private readonly StorageStartupOperations _startupOperations;
    private readonly StorageReadOperations _readOperations;
    private readonly StorageWriteOperations _writeOperations;
    private readonly StorageIndexOperations _indexOperations;
    private readonly StorageMaintenanceOperations _maintenanceOperations;
    private readonly AsyncReaderWriterLock _lock = new();
    private readonly FileLock? _fileLock;

    internal StorageEngine(
        IRegionProvider regions,
        IWriteAheadLog wal,
        ISecondaryIndexManager secondaryIndex,
        CheckpointHandle checkpointHandle,
        StorageEngineLifetime lifetime,
        StorageStartupOperations startupOperations,
        StorageReadOperations readOperations,
        StorageWriteOperations writeOperations,
        StorageIndexOperations indexOperations,
        StorageMaintenanceOperations maintenanceOperations,
        FileLock? fileLock = null)
    {
        ArgumentNullException.ThrowIfNull(regions);
        ArgumentNullException.ThrowIfNull(wal);
        ArgumentNullException.ThrowIfNull(secondaryIndex);
        ArgumentNullException.ThrowIfNull(checkpointHandle);
        ArgumentNullException.ThrowIfNull(lifetime);
        ArgumentNullException.ThrowIfNull(startupOperations);
        ArgumentNullException.ThrowIfNull(readOperations);
        ArgumentNullException.ThrowIfNull(writeOperations);
        ArgumentNullException.ThrowIfNull(indexOperations);
        ArgumentNullException.ThrowIfNull(maintenanceOperations);

        _regions = regions;
        _wal = wal;
        _secondaryIndex = secondaryIndex;
        _checkpointHandle = checkpointHandle;
        _lifetime = lifetime;
        _startupOperations = startupOperations;
        _readOperations = readOperations;
        _writeOperations = writeOperations;
        _indexOperations = indexOperations;
        _maintenanceOperations = maintenanceOperations;
        _fileLock = fileLock;
    }

    /// <summary>
    /// Initializes regions, replays WAL, and restores secondary index state.
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        _startupOperations.Initialize();
    }

    /// <summary>
    /// Saves one raw payload by primary key.
    /// </summary>
    public async Task SaveAsync(string table, Guid key, byte[] data, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        await _writeOperations.SaveAsync(table, key, data).ConfigureAwait(false);
    }

    /// <summary>
    /// Saves data with indexed field values for secondary indexes.
    /// </summary>
    public async Task SaveAsync(string table, Guid key, byte[] data, IReadOnlyDictionary<string, string> indexedFields, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        await _writeOperations.SaveAsync(table, key, data, indexedFields).ConfigureAwait(false);
    }

    /// <summary>
    /// Saves a pre-serialized batch of records for one table.
    /// </summary>
    public async Task SaveBatchAsync(string table, IReadOnlyCollection<StorageWriteEntry> entries, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        await _writeOperations.SaveBatchAsync(table, entries).ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes one record by primary key.
    /// </summary>
    public async Task DeleteAsync(string table, Guid key, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        await _writeOperations.DeleteAsync(table, key).ConfigureAwait(false);
    }

    /// <summary>
    /// Drops all records from the specified table.
    /// </summary>
    public async Task<long> DropTableAsync(string table, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _writeOperations.DropTableAsync(table).ConfigureAwait(false);
    }

    /// <summary>
    /// Removes all records from a table but the table continues to exist.
    /// </summary>
    public async Task<long> TruncateTableAsync(string table, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _writeOperations.TruncateTableAsync(table).ConfigureAwait(false);
    }

    /// <summary>
    /// Rewrites storage files to reclaim space for selected tables or for all tables when the input array is empty.
    /// </summary>
    public async Task<long> CompactAsync(string[] tables, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _maintenanceOperations.CompactAsync(tables).ConfigureAwait(false);
    }

    /// <summary>
    /// Drops a secondary index for the specified table field.
    /// </summary>
    public async Task DropIndexAsync(string table, string fieldName, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        await _indexOperations.DropIndexAsync(table, fieldName).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns active secondary indexes for the specified table.
    /// </summary>
    public async Task<IReadOnlyList<IndexDefinition>> GetIndexesAsync(string table, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireReadLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _indexOperations.GetIndexesAsync(table).ConfigureAwait(false);
    }

    /// <summary>
    /// Looks up record keys using a secondary index.
    /// </summary>
    public async Task<List<Guid>?> LookupByIndexAsync(string table, string fieldName, string value, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireReadLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _indexOperations.LookupByIndexAsync(table, fieldName, value).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads one record by primary key.
    /// </summary>
    public async Task<StorageRecord?> GetByKeyAsync(string table, Guid key, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireReadLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _readOperations.GetByKeyAsync(table, key).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns records for a table with skip/take pagination.
    /// </summary>
    public async Task<List<StorageRecord>> GetByTableAsync(string table, int skip = 0, int take = int.MaxValue, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireReadLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _readOperations.GetByTableAsync(table, skip, take, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Streams records for a table lazily without pre-buffering a list of records.
    /// </summary>
    public async IAsyncEnumerable<StorageRecord> GetByTableStreamAsync(
        string table,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireReadLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        await foreach (var record in _readOperations.GetByTableStreamAsync(table, cancellationToken).ConfigureAwait(false))
        {
            yield return record;
        }
    }

    /// <summary>
    /// Returns the number of live records in the specified table.
    /// </summary>
    public async Task<long> CountAsync(string table, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireReadLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _readOperations.CountAsync(table).ConfigureAwait(false);
    }

    /// <summary>
    /// Returns names of tables that currently contain live records.
    /// </summary>
    public async Task<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireReadLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _readOperations.ListTablesAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Returns <c>true</c> if a table currently exists and contains live records.
    /// </summary>
    public async Task<bool> TableExistsAsync(string table, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireReadLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _readOperations.TableExistsAsync(table).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads multiple records by keys under one read lock with skip/take semantics.
    /// </summary>
    public async Task<List<StorageRecord>> GetByKeysAsync(
        string table,
        IReadOnlyList<Guid> keys,
        int skip = 0,
        int take = int.MaxValue,
        CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireReadLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        return await _readOperations.GetByKeysAsync(table, keys, skip, take, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Ensures a secondary index exists for the specified table field.
    /// </summary>
    public async Task EnsureIndexAsync(string table, string fieldName, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        await _indexOperations.EnsureIndexAsync(table, fieldName).ConfigureAwait(false);
    }

    /// <summary>
    /// Flushes pending checkpoint state and releases all engine-owned resources.
    /// </summary>
    public void Dispose()
    {
        if (!_lifetime.TryBeginDispose())
            return;

        using var writeLock = _lock.AcquireWriteLock();
        try
        {
            try
            {
                _checkpointHandle.Current.ForceCheckpoint();
            }
            catch (ObjectDisposedException)
            {
            }

            if (_secondaryIndex is IDisposable disposable)
                disposable.Dispose();

            _wal.Dispose();
            _regions.Dispose();
        }
        finally
        {
            _lifetime.MarkDisposed();
            _fileLock?.Dispose();
            _lock.Dispose();
        }
    }

    /// <summary>
    /// Deletes a batch of records by primary keys.
    /// </summary>
    public async Task DeleteBatchAsync(string table, IEnumerable<Guid> keys, CancellationToken cancellationToken = default)
    {
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        using var _ = await _lock.AcquireWriteLockAsync(cancellationToken).ConfigureAwait(false);
        _lifetime.ThrowIfNotActive(typeof(StorageEngine));

        await _writeOperations.DeleteBatchAsync(table, keys).ConfigureAwait(false);
    }
}
