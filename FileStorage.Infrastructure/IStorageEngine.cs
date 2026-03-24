using FileStorage.Abstractions;
using FileStorage.Abstractions.SecondaryIndex;

namespace FileStorage.Infrastructure;

/// <summary>
/// Core storage engine contract. Operates on raw byte[] data only — no
/// business logic (filtering, encoding) belongs here.
/// </summary>
internal interface IStorageEngine : IDisposable
{
    Task SaveAsync(string table, Guid key, byte[] data, CancellationToken cancellationToken = default);

    /// <summary>
    /// Saves data with indexed field values for secondary indexes.
    /// </summary>
    Task SaveAsync(string table, Guid key, byte[] data, IReadOnlyDictionary<string, string> indexedFields, CancellationToken cancellationToken = default);

    Task<StorageRecord?> GetByKeyAsync(string table, Guid key, CancellationToken cancellationToken = default);
    Task DeleteAsync(string table, Guid key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns records for a table with skip/take pagination.
    /// Content-level filtering is the caller's responsibility.
    /// </summary>
    Task<List<StorageRecord>> GetByTableAsync(string table, int skip = 0, int take = int.MaxValue, CancellationToken cancellationToken = default);

    Task<long> CountAsync(string table, CancellationToken cancellationToken = default);
    Task InitializeAsync(CancellationToken cancellationToken = default);
    Task<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default);
    Task<bool> TableExistsAsync(string table, CancellationToken cancellationToken = default);
    Task<long> DropTableAsync(string table, CancellationToken cancellationToken = default);

    /// <summary>
    /// Removes all records from a table but the table continues to exist.
    /// Returns the number of records removed. Disk space is reclaimed by <see cref="CompactAsync"/>.
    /// </summary>
    Task<long> TruncateTableAsync(string table, CancellationToken cancellationToken = default);

    Task<long> CompactAsync(string[] tables, CancellationToken cancellationToken = default);

    // ── Secondary index operations ──
    Task DropIndexAsync(string table, string fieldName, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<IndexDefinition>> GetIndexesAsync(string table, CancellationToken cancellationToken = default);

    /// <summary>
    /// Looks up record keys using a secondary index.
    /// Returns <c>null</c> if no index exists for the field, allowing the caller to fall back to a full scan.
    /// </summary>
    Task<List<Guid>?> LookupByIndexAsync(string table, string fieldName, string value, CancellationToken cancellationToken = default);
    Task EnsureIndexAsync(string table, string fieldName, CancellationToken cancellationToken = default);
}