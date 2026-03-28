namespace FileStorage.Abstractions;

/// <summary>
/// Represents one stored record.
/// </summary>
public sealed record StorageRecord(
    string TableName,
    Guid Key,
    byte[] Data,
    long Version,
    bool IsDeleted,
    long? ExpiresAt = null);