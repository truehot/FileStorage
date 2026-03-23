using FileStorage.Abstractions.SecondaryIndex;

namespace FileStorage.Infrastructure.Indexing.SecondaryIndex;

/// <summary>
/// Manages secondary indexes for all tables.
/// Coordinates MemTable writes, SSTable flushes, and lookups.
/// </summary>
internal interface ISecondaryIndexManager
{
    /// <summary>
    /// Drops an index and deletes all associated SSTable files.
    /// </summary>
    void DropIndex(string table, string fieldName);

    /// <summary>
    /// Returns all active index definitions for a table.
    /// </summary>
    IReadOnlyList<IndexDefinition> GetIndexes(string table);

    /// <summary>
    /// Returns true if an index exists for this table+field.
    /// </summary>
    bool HasIndex(string table, string fieldName);

    /// <summary>
    /// Inserts indexed field values for a record.
    /// Only fields with an active index are stored.
    /// </summary>
    void Put(string table, Guid recordKey, IReadOnlyDictionary<string, string> indexedFields);

    /// <summary>
    /// Removes a record from all secondary indexes for the table using known previous values.
    /// </summary>
    void Remove(string table, Guid recordKey, IReadOnlyDictionary<string, string> previousValues);

    /// <summary>
    /// Removes a record from all secondary indexes for the table.
    /// Scans MemTables to find and remove entries for this key.
    /// Used when previous indexed values are not available (e.g. Delete by primary key).
    /// </summary>
    void RemoveByKey(string table, Guid recordKey);

    /// <summary>
    /// Looks up record keys by exact field value.
    /// Searches MemTable first, then all SSTables.
    /// </summary>
    List<Guid> Lookup(string table, string fieldName, string value);

    /// <summary>
    /// Drops all indexes for a table (used on table drop/truncate).
    /// </summary>
    void DropAllIndexes(string table);

    /// <summary>
    /// Ensures a secondary index exists for a table+field.
    /// If the index already exists, this is a no-op.
    /// </summary>
    void EnsureIndex(string table, string fieldName);
}