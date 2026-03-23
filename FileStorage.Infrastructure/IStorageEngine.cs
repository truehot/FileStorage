using FileStorage.Abstractions;
using FileStorage.Abstractions.SecondaryIndex;

namespace FileStorage.Infrastructure;

/// <summary>
/// Core storage engine contract. Operates on raw byte[] data only — no
/// business logic (filtering, encoding) belongs here.
/// </summary>
internal interface IStorageEngine : IDisposable
{
    Task SaveAsync(string table, Guid key, byte[] data);

    /// <summary>
    /// Saves data with indexed field values for secondary indexes.
    /// </summary>
    Task SaveAsync(string table, Guid key, byte[] data, IReadOnlyDictionary<string, string> indexedFields);

    Task<StorageRecord?> GetByKeyAsync(string table, Guid key);
    Task DeleteAsync(string table, Guid key);

    /// <summary>
    /// Returns records for a table with skip/take pagination.
    /// Content-level filtering is the caller's responsibility.
    /// </summary>
    Task<List<StorageRecord>> GetByTableAsync(string table, int skip = 0, int take = int.MaxValue);

    Task<long> CountAsync(string table);
    Task InitializeAsync();
    Task<IReadOnlyList<string>> ListTablesAsync();
    Task<bool> TableExistsAsync(string table);
    Task<long> DropTableAsync(string table);

    /// <summary>
    /// Removes all records from a table but the table continues to exist.
    /// Returns the number of records removed. Disk space is reclaimed by <see cref="CompactAsync"/>.
    /// </summary>
    Task<long> TruncateTableAsync(string table);

    Task<long> CompactAsync(params string[] tables);

    // ── Secondary index operations ──
    Task DropIndexAsync(string table, string fieldName);
    Task<IReadOnlyList<IndexDefinition>> GetIndexesAsync(string table);

    /// <summary>
    /// Looks up record keys using a secondary index.
    /// Returns <c>null</c> if no index exists for the field, allowing the caller to fall back to a full scan.
    /// </summary>
    Task<List<Guid>?> LookupByIndexAsync(string table, string fieldName, string value);
    Task EnsureIndexAsync(string table, string fieldName);
}