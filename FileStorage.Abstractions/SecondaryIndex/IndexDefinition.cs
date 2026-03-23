namespace FileStorage.Abstractions.SecondaryIndex;

/// <summary>
/// Describes a secondary index on a table field.
/// </summary>
public sealed class IndexDefinition
{
    /// <summary>
    /// Name of the indexed JSON field (e.g. "email", "status").
    /// </summary>
    public required string FieldName { get; init; }

    /// <summary>
    /// When the index was created (UTC ticks).
    /// </summary>
    public required long CreatedAtUtc { get; init; }
}