namespace FileStorage.Abstractions.SecondaryIndex;

/// <summary>
/// Metadata about a table: record count and active indexes.
/// </summary>
public sealed class TableInfo
{
    public required string TableName { get; init; }
    public required long RecordCount { get; init; }
    public required IReadOnlyList<IndexDefinition> Indexes { get; init; }
}