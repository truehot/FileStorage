namespace FileStorage.Abstractions;

/// <summary>
/// Represents a record stored in a table.
/// </summary>
public sealed class StorageRecord
{
    /// <summary>
    /// The name of the table containing this record.
    /// </summary>
    public required string TableName { get; set; }

    /// <summary>
    /// The unique key of the record.
    /// </summary>
    public required Guid Key { get; set; }

    /// <summary>
    /// The raw data of the record.
    /// </summary>
    public required byte[] Data { get; set; }

    /// <summary>
    /// The expiration timestamp (Unix time, seconds), or null if the record does not expire.
    /// </summary>
    public long? ExpiresAt { get; set; }

    /// <summary>
    /// The version of the record (for concurrency control).
    /// </summary>
    public required long Version { get; set; }

    /// <summary>
    /// Indicates whether the record is soft-deleted.
    /// </summary>
    public required bool IsDeleted { get; set; }
}