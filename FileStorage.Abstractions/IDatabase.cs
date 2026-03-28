namespace FileStorage.Abstractions;

/// <summary>
/// Database-level operations: table management, compaction, lifecycle.
/// </summary>
public interface IDatabase : IAsyncDisposable, IDisposable
{
    /// <summary>
    /// Opens a table handle for record-level operations. The table is created implicitly on first write.
    /// Throws <see cref="ArgumentException"/> if <paramref name="name"/> is null or empty.
    /// </summary>
    ITable OpenTable(string name);

    /// <summary>
    /// Returns the names of all tables that contain at least one record.
    /// Cancellation is best-effort.
    /// </summary>
    Task<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Returns true if the table exists and contains at least one record.
    /// Throws <see cref="ArgumentException"/> if <paramref name="name"/> is null or empty.
    /// Cancellation is best-effort.
    /// </summary>
    Task<bool> TableExistsAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Drops an entire table by soft-deleting all its records. Returns the number of records removed.
    /// Throws <see cref="ArgumentException"/> if <paramref name="name"/> is null or empty.
    /// Cancellation is best-effort.
    /// </summary>
    Task<long> DropTableAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reclaims disk space by rewriting files without soft-deleted records. Cancellation is best-effort.
    /// </summary>
    Task<long> CompactAsync(string[]? tables = null, CancellationToken cancellationToken = default);
}