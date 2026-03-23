namespace FileStorage.Infrastructure.Indexing.SecondaryIndex;

/// <summary>
/// One entry in the sparse index: maps a sampled key to the start of a 4 KB block in the SSTable file.
/// </summary>
internal readonly record struct SparseIndexEntry(string Key, long BlockOffset);