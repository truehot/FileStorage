namespace FileStorage.Abstractions.SecondaryIndex;

/// <summary>
/// Metadata about a table: record count and active indexes.
/// </summary>
public sealed class TableInfo
{
    /// <summary>
    /// The name of the table.
    /// </summary>
    public required string TableName { get; init; }

    /// <summary>
    /// The number of records in the table.
    /// </summary>
    public required long RecordCount { get; init; }

    /// <summary>
    /// The list of active secondary indexes for the table.
    /// </summary>
    public required IReadOnlyList<IndexDefinition> Indexes { get; init; }
}