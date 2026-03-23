namespace FileStorage.Abstractions;

public sealed class StorageRecord
{
    public required string TableName { get; set; }
    public required Guid Key { get; set; }
    public required byte[] Data { get; set; }
    public long? ExpiresAt { get; set; }
    public required long Version { get; set; }
    public required bool IsDeleted { get; set; }
}