using System.Collections.Concurrent;

namespace FileStorage.Infrastructure.Indexing.Primary;

/// <summary>
/// Thread-safe in-memory index that maps (table, key) pairs to file offsets.
/// Supports lookup, pagination by table, and concurrent add/remove operations.
/// <para>
/// Uses <see cref="HashSet{T}"/> for per-table key sets instead of <c>List</c>,
/// giving O(1) remove instead of O(N) — critical for tables with millions of keys.
/// </para>
/// </summary>
internal sealed class MemoryIndex : IMemoryIndex
{
    private readonly ConcurrentDictionary<(string Table, Guid Key), long> _primary = new();
    private readonly ConcurrentDictionary<string, HashSet<Guid>> _byTable = new();
    private readonly Lock _tableLock = new();

    public int Count => _primary.Count;

    public bool TryGet(string table, Guid key, out long offset) =>
        _primary.TryGetValue((table, key), out offset);

    public void AddOrUpdate(string table, Guid key, long offset)
    {
        lock (_tableLock)
        {
            bool isNew = !_primary.ContainsKey((table, key));
            _primary[(table, key)] = offset;

            if (isNew)
            {
                var set = _byTable.GetOrAdd(table, _ => []);
                set.Add(key);
            }
        }
    }

    public bool TryRemove(string table, Guid key)
    {
        lock (_tableLock)
        {
            if (!_primary.TryRemove((table, key), out _)) return false;

            if (_byTable.TryGetValue(table, out var set))
                set.Remove(key); // O(1) instead of O(N)

            return true;
        }
    }

    public void Clear()
    {
        lock (_tableLock)
        {
            _primary.Clear();
            _byTable.Clear();
        }
    }

    /// <summary>
    /// Returns keys for a specific table with skip/take applied in-memory.
    /// </summary>
    public IReadOnlyList<(Guid Key, long Offset)> GetByTable(string table, int skip = 0, int take = int.MaxValue)
    {
        if (!_byTable.TryGetValue(table, out var keys))
            return [];

        lock (_tableLock)
        {
            var result = new List<(Guid, long)>(Math.Min(take, keys.Count));
            int skipped = 0;
            foreach (var key in keys)
            {
                if (result.Count >= take) break;
                if (!_primary.TryGetValue((table, key), out long offset)) continue;
                if (skipped < skip) { skipped++; continue; }
                result.Add((key, offset));
            }
            return result;
        }
    }

    /// <summary>
    /// Returns count of live keys for a specific table.
    /// O(1) — <see cref="HashSet{T}.Count"/> is cached.
    /// </summary>
    public long CountByTable(string table)
    {
        if (!_byTable.TryGetValue(table, out var keys)) return 0;

        lock (_tableLock)
        {
            return keys.Count;
        }
    }

    /// <summary>
    /// Returns the names of all tables that currently have at least one live key.
    /// </summary>
    public IReadOnlyList<string> GetTableNames()
    {
        lock (_tableLock)
        {
            var result = new List<string>();
            foreach (var (table, keys) in _byTable)
            {
                if (keys.Count > 0)
                    result.Add(table);
            }
            return result;
        }
    }

    /// <summary>
    /// Returns <c>true</c> if the table exists and contains at least one live key.
    /// </summary>
    public bool TableExists(string table)
    {
        if (!_byTable.TryGetValue(table, out var keys)) return false;

        lock (_tableLock)
        {
            return keys.Count > 0;
        }
    }

    /// <summary>
    /// Removes all keys belonging to the specified table from the index.
    /// Returns the list of (key, offset) pairs that were removed.
    /// </summary>
    public IReadOnlyList<(Guid Key, long Offset)> RemoveTable(string table)
    {
        lock (_tableLock)
        {
            if (!_byTable.TryRemove(table, out var keys))
                return [];

            var removed = new List<(Guid, long)>(keys.Count);
            foreach (var key in keys)
            {
                if (_primary.TryRemove((table, key), out long offset))
                    removed.Add((key, offset));
            }
            return removed;
        }
    }
}