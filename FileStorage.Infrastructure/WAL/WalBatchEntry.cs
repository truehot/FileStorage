namespace FileStorage.Infrastructure.WAL;

/// <summary>
/// In-memory representation of one item inside a WAL batch payload.
/// </summary>
internal readonly record struct WalBatchEntry(
    Guid Key,
    byte[] Data,
    long DataOffset,
    long IndexOffset,
    IReadOnlyDictionary<string, string> IndexedFields);
