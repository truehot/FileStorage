namespace FileStorage.Infrastructure.Core.Models;

/// <summary>
/// Pre-serialized write entry for batch operations.
/// </summary>
internal readonly record struct StorageWriteEntry(
    Guid Key,
    byte[] Data,
    IReadOnlyDictionary<string, string> IndexedFields);
