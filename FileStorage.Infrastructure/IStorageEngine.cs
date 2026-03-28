using FileStorage.Abstractions;
using FileStorage.Abstractions.SecondaryIndex;
using FileStorage.Infrastructure.Core.Models;

namespace FileStorage.Infrastructure;

/// <summary>
/// Core storage engine contract. Operates on raw byte[] data only — no
/// business logic (filtering, encoding) belongs here.
/// </summary>
internal interface IStorageEngine : IDisposable
{
    /// <summary>
    /// Saves one raw payload by primary key.
    /// </summary>
    Task SaveAsync(string table, Guid key, byte[] data, CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves data with indexed field values for secondary indexes.
    /// </summary>
    Task SaveAsync(string table, Guid key, byte[] data, IReadOnlyDictionary<string, string> indexedFields, CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves a pre-serialized batch of records for one table.
    /// </summary>
    Task SaveBatchAsync(string table, IReadOnlyCollection<StorageWriteEntry> entries, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads one record by primary key.
    /// </summary>
    Task<StorageRecord?> GetByKeyAsync(string table, Guid key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads multiple records by keys under one read lock with skip/take semantics.
    /// </summary>
    Task<List<StorageRecord>> GetByKeysAsync(
        string table,
        IReadOnlyList<Guid> keys,
        int skip = 0,
        int take = int.MaxValue,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes one record by primary key.
    /// </summary>
    Task DeleteAsync(string table, Guid key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a batch of records by their keys. Atomic and crash-safe (single WAL entry).
    /// </summary>
    Task DeleteBatchAsync(string table, IEnumerable<Guid> keys, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns records for a table with skip/take pagination.
    /// Content-level filtering is the caller's responsibility.
    /// </summary>
    Task<List<StorageRecord>> GetByTableAsync(string table, int skip = 0, int take = int.MaxValue, CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams records for a table lazily without pre-buffering a <see cref="List{T}"/> of records.
    /// Content-level filtering is the caller's responsibility.
    /// </summary>
    IAsyncEnumerable<StorageRecord> GetByTableStreamAsync(string table, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the number of live records in the specified table.
    /// </summary>
    Task<long> CountAsync(string table, CancellationToken cancellationToken = default);

    /// <summary>
    /// Initializes storage regions and recovery state.
    /// Must be called before read/write operations.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns names of tables that currently contain live records.
    /// </summary>
    Task<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns <c>true</c> if a table currently exists and contains live records.
    /// </summary>
    Task<bool> TableExistsAsync(string table, CancellationToken cancellationToken = default);

    /// <summary>
    /// Drops all records from the specified table.
    /// Returns the number of removed records.
    /// </summary>
    Task<long> DropTableAsync(string table, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes all records from a table but the table continues to exist.
    /// Returns the number of records removed. Disk space is reclaimed by <see cref="CompactAsync"/>.
    /// </summary>
    Task<long> TruncateTableAsync(string table, CancellationToken cancellationToken = default);

    /// <summary>
    /// Rewrites storage files to reclaim space for selected tables or for all tables when the input array is empty.
    /// </summary>
    Task<long> CompactAsync(string[] tables, CancellationToken cancellationToken = default);

    // ── Secondary index operations ──
    /// <summary>
    /// Drops a secondary index for the specified table field.
    /// </summary>
    Task DropIndexAsync(string table, string fieldName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns active secondary indexes for the specified table.
    /// </summary>
    Task<IReadOnlyList<IndexDefinition>> GetIndexesAsync(string table, CancellationToken cancellationToken = default);

    /// <summary>
    /// Looks up record keys using a secondary index.
    /// Returns <c>null</c> if no index exists for the field, allowing the caller to fall back to a full scan.
    /// </summary>
    Task<List<Guid>?> LookupByIndexAsync(string table, string fieldName, string value, CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures a secondary index exists for the specified table field.
    /// </summary>
    Task EnsureIndexAsync(string table, string fieldName, CancellationToken cancellationToken = default);
}