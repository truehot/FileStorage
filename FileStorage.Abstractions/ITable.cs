namespace FileStorage.Abstractions;

using FileStorage.Abstractions.SecondaryIndex;

/// <summary>
/// Table-level operations: CRUD, querying, and secondary index management.
/// </summary>
public interface ITable
{
    /// <summary>
    /// The table name.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Saves a record with the specified key and data.
    /// </summary>
    Task SaveAsync(Guid key, string data, CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves a record with explicit indexed field values.
    /// Values are inserted into secondary indexes defined for this table.
    /// Fields without a matching index definition are ignored.
    /// </summary>
    Task SaveAsync(Guid key, string data, IReadOnlyDictionary<string, string> indexedFields, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a record by key.
    /// </summary>
    Task<StorageRecord?> GetAsync(Guid key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a record by key.
    /// </summary>
    Task DeleteAsync(Guid key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns records with optional content-level text filtering and pagination.
    /// Uses secondary indexes when <paramref name="filterField"/> matches an active index.
    /// </summary>
    Task<List<StorageRecord>> FilterAsync(
        string? filterField = null,
        string? filterValue = null,
        int skip = 0,
        int take = int.MaxValue,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Streams records with optional content-level text filtering.
    /// Yields results one-by-one without buffering the entire result set.
    /// </summary>
    IAsyncEnumerable<StorageRecord> StreamAsync(string? filterValue = null, CancellationToken cancellationToken = default);

    // ── Index management ──

    /// <summary>
    /// Returns table metadata including record count and active indexes.
    /// </summary>
    Task<TableInfo> GetTableInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Drops a secondary index. Removes all SSTable files and in-memory data for the field.
    /// </summary>
    Task DropIndexAsync(string fieldName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes all records from the table. The table continues to exist (returns empty on queries).
    /// Returns the number of records removed. Disk space is reclaimed by compaction.
    /// </summary>
    Task<long> TruncateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the number of records in the table.
    /// </summary>
    Task<long> CountAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures a secondary index exists on the specified field.
    /// If the index already exists, this is a no-op.
    /// Existing records are NOT retroactively indexed — only new writes.
    /// </summary>
    Task EnsureIndexAsync(string fieldName, CancellationToken cancellationToken = default);
}