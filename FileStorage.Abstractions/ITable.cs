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
    /// Saves a record with the specified key and string data.
    /// </summary>
    Task SaveAsync(
        Guid key,
        string data,
        IReadOnlyDictionary<string, string>? indexedFields = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves a record with the specified key and binary data.
    /// </summary>
    Task SaveAsync(
        Guid key,
        byte[] data,
        IReadOnlyDictionary<string, string>? indexedFields = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves a record with the specified key and generic data using a string serializer.
    /// </summary>
    Task SaveAsync<T>(
        Guid key,
        T item,
        Func<T, string> dataSelector,
        IReadOnlyDictionary<string, string>? indexedFields = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves a record with the specified key and generic data using a byte[] serializer.
    /// </summary>
    Task SaveAsync<T>(
        Guid key,
        T item,
        Func<T, byte[]> dataSelector,
        IReadOnlyDictionary<string, string>? indexedFields = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves a generic batch. Serialization to string happens at application layer.
    /// </summary>
    Task SaveBatchAsync<T>(
        IReadOnlyCollection<T> items,
        Func<T, Guid> keySelector,
        Func<T, string> dataSelector,
        Func<T, IReadOnlyDictionary<string, string>>? indexedFieldsSelector = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves a generic batch. Serialization to byte[] happens at application layer.
    /// </summary>
    Task SaveBatchAsync<T>(
        IReadOnlyCollection<T> items,
        Func<T, Guid> keySelector,
        Func<T, byte[]> dataSelector,
        Func<T, IReadOnlyDictionary<string, string>>? indexedFieldsSelector = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a record by key.
    /// Throws <see cref="ArgumentException"/> if <paramref name="key"/> is empty.
    /// Cancellation is best-effort.
    /// </summary>
    Task<StorageRecord?> GetAsync(Guid key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a record by key.
    /// Throws <see cref="ArgumentException"/> if <paramref name="key"/> is empty.
    /// Cancellation is best-effort.
    /// </summary>
    Task DeleteAsync(Guid key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns records with optional content-level text filtering and pagination.
    /// Uses secondary indexes when <paramref name="filterField"/> matches an active index.
    /// Throws <see cref="ArgumentOutOfRangeException"/> if <paramref name="skip"/> or <paramref name="take"/> is negative.
    /// Throws <see cref="ArgumentException"/> if <paramref name="filterField"/> is provided but <paramref name="filterValue"/> is null.
    /// Cancellation is best-effort.
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
    /// Cancellation is best-effort.
    /// </summary>
    IAsyncEnumerable<StorageRecord> StreamAsync(string? filterValue = null, CancellationToken cancellationToken = default);

    // ── Index management ──

    /// <summary>
    /// Returns table metadata including record count and active indexes.
    /// Cancellation is best-effort.
    /// </summary>
    Task<TableInfo> GetTableInfoAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Drops a secondary index for the specified field.
    /// Throws <see cref="ArgumentException"/> if <paramref name="fieldName"/> is null or empty.
    /// Cancellation is best-effort.
    /// </summary>
    Task DropIndexAsync(string fieldName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes all records from the table. The table continues to exist (returns empty on queries).
    /// Returns the number of records removed. Disk space is reclaimed by compaction.
    /// Cancellation is best-effort.
    /// </summary>
    Task<long> TruncateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns the number of records in the table.
    /// Cancellation is best-effort.
    /// </summary>
    Task<long> CountAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures a secondary index exists on the specified field.
    /// If the index already exists, this is a no-op.
    /// Existing records are NOT retroactively indexed — only new writes.
    /// Throws <see cref="ArgumentException"/> if <paramref name="fieldName"/> is null or empty.
    /// Cancellation is best-effort.
    /// </summary>
    Task EnsureIndexAsync(string fieldName, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a batch of records by their keys. Atomic and crash-safe (single WAL entry).
    /// </summary>
    Task DeleteBatchAsync(IEnumerable<Guid> keys, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes a batch of records by selector from a collection of items (generic). Atomic and crash-safe (single WAL entry).
    /// </summary>
    Task DeleteBatchAsync<T>(IEnumerable<T> items, Func<T, Guid> keySelector, CancellationToken cancellationToken = default);
}