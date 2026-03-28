namespace FileStorage.Infrastructure.Indexing.SecondaryIndex;

/// <summary>
/// In-memory sparse index over an SSTable file.
/// Holds every Nth key with its block-aligned file offset, enabling binary search
/// to locate the approximate 4 KB block, then a short linear scan within it.
/// </summary>
internal sealed class SparseIndex(List<SparseIndexEntry> entries)
{
    private readonly List<SparseIndexEntry> _entries = entries;

    public IReadOnlyList<SparseIndexEntry> Entries => _entries;

    /// <summary>
    /// Finds the block-aligned SSTable offset to start scanning from for the given key.
    /// Returns the offset of the sparse entry at or just before the target key.
    /// </summary>
    public long FindStartOffset(string key)
    {
        int lo = 0, hi = _entries.Count - 1;
        long bestOffset = 0;

        while (lo <= hi)
        {
            int mid = lo + (hi - lo) / 2;
            int cmp = string.Compare(_entries[mid].Key, key, StringComparison.Ordinal);

            if (cmp <= 0)
            {
                bestOffset = _entries[mid].BlockOffset;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return bestOffset;
    }
}