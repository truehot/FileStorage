namespace FileStorage.Abstractions;

/// <summary>
/// Database-level operations: table management, compaction, lifecycle.
/// </summary>
public interface IDatabase : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Opens a table handle for record-level operations.
    /// The table is created implicitly on first write.
    /// </summary>
    ITable OpenTable(string name);

    /// <summary>
    /// Returns the names of all tables that contain at least one record.
    /// </summary>
    Task<IReadOnlyList<string>> ListTablesAsync();

    /// <summary>
    /// Returns <c>true</c> if the table exists and contains at least one record.
    /// </summary>
    Task<bool> TableExistsAsync(string name);

    /// <summary>
    /// Drops an entire table by soft-deleting all its records.
    /// Returns the number of records removed.
    /// </summary>
    Task<long> DropTableAsync(string name);

    /// <summary>
    /// Reclaims disk space by rewriting files without soft-deleted records.
    /// Pass table names to compact selectively, or omit to compact all.
    /// Uses atomic file rename — fully crash-safe.
    /// </summary>
    Task<long> CompactAsync(params string[] tables);
}