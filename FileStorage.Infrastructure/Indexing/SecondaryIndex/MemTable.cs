namespace FileStorage.Infrastructure.Indexing.SecondaryIndex;

/// <summary>
/// In-memory sorted buffer for a single field index.
/// When <see cref="Count"/> reaches the flush threshold, the engine
/// freezes this MemTable and writes it as an immutable SSTable.
/// 
/// Key: indexed field value, Value: set of record Guids.
/// Sorted by key for efficient range scans and SSTable merge.
/// </summary>
internal sealed class MemTable
{
    private readonly SortedDictionary<string, HashSet<Guid>> _entries = new(StringComparer.Ordinal);
    private readonly Lock _lock = new();

    public int Count
    {
        get { lock (_lock) { return _entries.Count; } }
    }

    /// <summary>
    /// Total number of individual key→Guid mappings.
    /// </summary>
    public long TotalMappings
    {
        get
        {
            lock (_lock)
            {
                long total = 0;
                foreach (var set in _entries.Values)
                    total += set.Count;
                return total;
            }
        }
    }

    public void Put(string value, Guid recordKey)
    {
        lock (_lock)
        {
            if (!_entries.TryGetValue(value, out var set))
            {
                set = [];
                _entries[value] = set;
            }
            set.Add(recordKey);
        }
    }

    public void Remove(string value, Guid recordKey)
    {
        lock (_lock)
        {
            if (_entries.TryGetValue(value, out var set))
            {
                set.Remove(recordKey);
                if (set.Count == 0)
                    _entries.Remove(value);
            }
        }
    }

    /// <summary>
    /// Removes a record key from ALL entries in this MemTable, regardless of value.
    /// Used when the previous indexed value is unknown (Delete by primary key).
    /// O(N) scan — acceptable because MemTable is small (flush threshold).
    /// </summary>
    public void RemoveByKey(Guid recordKey)
    {
        lock (_lock)
        {
            var emptyKeys = new List<string>();
            foreach (var (value, set) in _entries)
            {
                set.Remove(recordKey);
                if (set.Count == 0)
                    emptyKeys.Add(value);
            }

            foreach (var key in emptyKeys)
                _entries.Remove(key);
        }
    }

    /// <summary>
    /// Returns all record keys matching the exact value.
    /// </summary>
    public List<Guid> Lookup(string value)
    {
        lock (_lock)
        {
            if (_entries.TryGetValue(value, out var set))
                return [.. set];
            return [];
        }
    }

    /// <summary>
    /// Takes a frozen snapshot of all entries for flushing to SSTable.
    /// The MemTable is cleared after this call.
    /// </summary>
    public SortedDictionary<string, List<Guid>> Freeze()
    {
        lock (_lock)
        {
            var snapshot = new SortedDictionary<string, List<Guid>>(StringComparer.Ordinal);
            foreach (var (key, set) in _entries)
                snapshot[key] = [.. set];
            _entries.Clear();
            return snapshot;
        }
    }
}