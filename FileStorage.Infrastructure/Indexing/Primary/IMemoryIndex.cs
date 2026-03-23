namespace FileStorage.Infrastructure.Indexing.Primary;

/// <summary>
/// Defines operations for an in-memory index mapping (table, key) pairs to file offsets.
/// </summary>
internal interface IMemoryIndex
{
    int Count { get; }
    bool TryGet(string table, Guid key, out long offset);
    void AddOrUpdate(string table, Guid key, long offset);
    bool TryRemove(string table, Guid key);
    void Clear();
    IReadOnlyList<(Guid Key, long Offset)> GetByTable(string table, int skip = 0, int take = int.MaxValue);
    long CountByTable(string table);

    /// <summary>
    /// Returns the names of all tables that currently have at least one live key.
    /// </summary>
    IReadOnlyList<string> GetTableNames();

    /// <summary>
    /// Returns <c>true</c> if the table exists and contains at least one live key.
    /// </summary>
    bool TableExists(string table);

    /// <summary>
    /// Removes all keys belonging to the specified table from the index.
    /// Returns the list of (key, offset) pairs that were removed.
    /// </summary>
    IReadOnlyList<(Guid Key, long Offset)> RemoveTable(string table);
}