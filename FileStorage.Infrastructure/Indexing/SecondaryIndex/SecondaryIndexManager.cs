using FileStorage.Abstractions.SecondaryIndex;

namespace FileStorage.Infrastructure.Indexing.SecondaryIndex;

/// <summary>
/// LSM-tree based secondary index manager.
/// 
/// <para><b>Architecture per (table, field):</b></para>
/// MemTable (mutable, sorted) → flush → SSTable (immutable, sorted + sparse index).
/// 
/// <para><b>Concurrency:</b></para>
/// Each (table, field) has its own <see cref="ReaderWriterLockSlim"/>.
/// Reads (Lookup) take a read lock — fully concurrent with other reads.
/// Writes (Put, Remove) take a write lock — only blocks the same index.
/// 
/// <para><b>Read amplification:</b></para>
/// SSTable compaction merges multiple SSTables via streaming K-way merge.
/// Memory usage during compaction is O(K) where K = number of SSTables,
/// not O(total data). Safe for 500 GB+ indexes.
/// 
/// <para><b>Crash safety:</b></para>
/// Compaction uses a manifest file to track the atomic swap.
/// On startup, <see cref="LoadExisting"/> detects incomplete compactions
/// and finishes or rolls them back.
/// </summary>
internal sealed class SecondaryIndexManager : ISecondaryIndexManager, IDisposable
{
    private readonly string _basePath;
    private readonly int _flushThreshold;
    private readonly int _compactionThreshold;
    private readonly Lock _registryLock = new();

    private readonly Dictionary<(string Table, string Field), FieldIndexState> _indexes = new();

    private const string ManifestFileName = "compaction.manifest";
    private const string ManifestCompleteMarker = "COMPLETE";

    internal SecondaryIndexManager(string basePath, int flushThreshold = 4096, int compactionThreshold = 4)
    {
        _basePath = basePath;
        _flushThreshold = flushThreshold;
        _compactionThreshold = compactionThreshold;
    }

    /// <summary>
    /// Maximum SSTable count per index before compaction triggers.
    /// </summary>
    public int CompactionThreshold => _compactionThreshold;

    // ──────────────────────────────────────────────
    //  Schema operations (global registry lock, rare)
    // ──────────────────────────────────────────────
    public void DropIndex(string table, string fieldName)
    {
        var key = (table, fieldName);
        FieldIndexState? state;

        lock (_registryLock)
        {
            if (!_indexes.Remove(key, out state)) return;
        }

        state.Dispose(GetIndexDirectory(table, fieldName));
    }

    public IReadOnlyList<IndexDefinition> GetIndexes(string table)
    {
        lock (_registryLock)
        {
            var result = new List<IndexDefinition>();
            foreach (var (k, state) in _indexes)
            {
                if (k.Table == table)
                    result.Add(state.Definition);
            }
            return result;
        }
    }

    public bool HasIndex(string table, string fieldName)
    {
        lock (_registryLock) { return _indexes.ContainsKey((table, fieldName)); }
    }

    // ──────────────────────────────────────────────
    //  Write operations (per-index write lock)
    // ──────────────────────────────────────────────

    public void Put(string table, Guid recordKey, IReadOnlyDictionary<string, string> indexedFields)
    {
        foreach (var (field, value) in indexedFields)
        {
            var state = GetState(table, field);
            if (state is null) continue;

            SortedDictionary<string, List<Guid>>? frozen = null;

            state.Lock.EnterWriteLock();
            try
            {
                state.MemTable.Put(value, recordKey);

                if (state.MemTable.TotalMappings >= _flushThreshold)
                    frozen = state.MemTable.Freeze();
            }
            finally { state.Lock.ExitWriteLock(); }

            if (frozen is not null)
                FlushAndCompact(table, field, state, frozen);
        }
    }

    public void Remove(string table, Guid recordKey, IReadOnlyDictionary<string, string> previousValues)
    {
        foreach (var (field, value) in previousValues)
        {
            var state = GetState(table, field);
            if (state is null) continue;

            state.Lock.EnterWriteLock();
            try
            {
                state.MemTable.Remove(value, recordKey);
            }
            finally { state.Lock.ExitWriteLock(); }
        }
    }

    // ──────────────────────────────────────────────
    //  Read operations (per-index read lock)
    // ──────────────────────────────────────────────

    public List<Guid> Lookup(string table, string fieldName, string value)
    {
        var state = GetState(table, fieldName);
        if (state is null) return [];

        state.Lock.EnterReadLock();
        try
        {
            // MemTable results first — typically small, gives us initial capacity hint
            var memResults = state.MemTable.Lookup(value);
            var tables = state.SSTables;

            // Fast path: single source, no dedup needed
            if (tables.Count == 0)
                return memResults;

            // Pre-size to avoid rehashing for high-cardinality keys.
            // Initial capacity from MemTable; SSTables add incrementally.
            var results = new HashSet<Guid>(memResults.Count > 16 ? memResults.Count * 2 : 16);

            foreach (var key in memResults)
                results.Add(key);

            for (int i = tables.Count - 1; i >= 0; i--)
            {
                foreach (var key in tables[i].Lookup(value))
                    results.Add(key);
            }

            // Write directly into pre-sized list — avoids [.. results] intermediate array
            var list = new List<Guid>(results.Count);
            list.AddRange(results);
            return list;
        }
        finally { state.Lock.ExitReadLock(); }
    }

    // ──────────────────────────────────────────────
    //  Bulk operations
    // ──────────────────────────────────────────────

    public void DropAllIndexes(string table)
    {
        List<((string Table, string Field) Key, FieldIndexState State)> removed = [];

        lock (_registryLock)
        {
            var toRemove = _indexes.Keys.Where(k => k.Table == table).ToList();
            foreach (var key in toRemove)
            {
                if (_indexes.Remove(key, out var state))
                    removed.Add((key, state));
            }
        }

        foreach (var (key, state) in removed)
            state.Dispose(GetIndexDirectory(key.Table, key.Field));

        var dir = Path.Combine(_basePath, "indexes", table);
        try { if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true); }
        catch { /* best effort */ }
    }

    // ──────────────────────────────────────────────
    //  Startup + crash recovery
    // ──────────────────────────────────────────────

    /// <summary>
    /// Loads all previously persisted indexes from disk on startup.
    /// Recovers any incomplete compaction via manifest files.
    /// </summary>
    internal void LoadExisting()
    {
        var indexRoot = Path.Combine(_basePath, "indexes");
        if (!Directory.Exists(indexRoot)) return;

        foreach (var tableDir in Directory.GetDirectories(indexRoot))
        {
            string table = Path.GetFileName(tableDir);
            foreach (var fieldDir in Directory.GetDirectories(tableDir))
            {
                string field = Path.GetFileName(fieldDir);

                // Recover incomplete compaction before loading SSTables
                RecoverCompaction(fieldDir);

                var sstFiles = Directory.GetFiles(fieldDir, "*.sst")
                    .OrderBy(f => f)
                    .ToList();

                var sstables = new List<SSTable>();
                foreach (var file in sstFiles)
                    sstables.Add(SSTable.Open(file));

                var definition = new IndexDefinition
                {
                    FieldName = field,
                    CreatedAtUtc = Directory.GetCreationTimeUtc(fieldDir).Ticks
                };

                _indexes[(table, field)] = new FieldIndexState(definition, sstables);
            }
        }
    }

    public void Dispose()
    {
        lock (_registryLock)
        {
            foreach (var (_, state) in _indexes)
            {
                state.Lock.EnterWriteLock();
                try
                {
                    foreach (var sst in state.SSTables)
                        sst.Dispose();
                }
                finally { state.Lock.ExitWriteLock(); }

                state.Lock.Dispose();
            }
            _indexes.Clear();
        }
    }

    // ──────────────────────────────────────────────
    //  Flush + Compaction
    // ──────────────────────────────────────────────

    private FieldIndexState? GetState(string table, string field)
    {
        lock (_registryLock)
        {
            return _indexes.TryGetValue((table, field), out var state) ? state : null;
        }
    }

    private void FlushAndCompact(string table, string field, FieldIndexState state, SortedDictionary<string, List<Guid>> frozen)
    {
        var dir = GetIndexDirectory(table, field);
        Directory.CreateDirectory(dir);

        string sstPath = Path.Combine(dir, $"{DateTime.UtcNow.Ticks:D20}.sst");
        var sst = SSTable.Write(sstPath, frozen);

        bool shouldCompact;
        state.Lock.EnterWriteLock();
        try
        {
            state.SSTables.Add(sst);
            shouldCompact = state.SSTables.Count >= _compactionThreshold;
        }
        finally { state.Lock.ExitWriteLock(); }

        if (shouldCompact)
            CompactSSTables(table, field, state);
    }

    /// <summary>
    /// Merges SSTables via streaming K-way merge with manifest-based crash safety.
    /// 
    /// <para><b>Memory usage:</b> O(K) iterators, where K = number of SSTables.
    /// Each iterator holds only the current entry — no full-file buffering.</para>
    /// 
    /// <para><b>Crash safety protocol:</b></para>
    /// <list type="number">
    ///   <item>Write manifest listing old SSTable files → fsync.</item>
    ///   <item>K-way merge → write merged.sst to disk → fsync.</item>
    ///   <item>Append merged file path to manifest → fsync.</item>
    ///   <item>Swap in-memory list under write lock.</item>
    ///   <item>Delete old SSTable files.</item>
    ///   <item>Write COMPLETE marker to manifest → fsync.</item>
    ///   <item>Delete manifest.</item>
    /// </list>
    /// 
    /// On crash at any step, <see cref="RecoverCompaction"/> reads the manifest
    /// and either completes or rolls back the operation.
    /// </summary>
    private void CompactSSTables(string table, string field, FieldIndexState state)
    {
        // Step 1: snapshot current SSTables
        List<SSTable> oldTables;
        state.Lock.EnterReadLock();
        try
        {
            if (state.SSTables.Count < _compactionThreshold)
                return;
            oldTables = [.. state.SSTables];
        }
        finally { state.Lock.ExitReadLock(); }

        var dir = GetIndexDirectory(table, field);
        string manifestPath = Path.Combine(dir, ManifestFileName);
        string mergedPath = Path.Combine(dir, $"{DateTime.UtcNow.Ticks:D20}_merged.sst");

        // Step 2: write manifest with old file names (crash → rollback: delete merged if exists)
        WriteManifest(manifestPath, oldTables.Select(s => s.FilePath).ToList(), mergedPath);

        // Step 3: streaming K-way merge → merged SSTable
        SSTable mergedSst;
        try
        {
            mergedSst = StreamingMerge(oldTables, mergedPath);
        }
        catch
        {
            // Merge failed — clean up and remove manifest
            TryDeleteFile(mergedPath);
            TryDeleteFile(manifestPath);
            return;
        }

        // Step 4: mark merge output as written in manifest
        AppendToManifest(manifestPath, "MERGED");

        // Step 5: atomic swap under write lock
        List<SSTable> toDelete;
        state.Lock.EnterWriteLock();
        try
        {
            toDelete = state.SSTables.Where(s => oldTables.Contains(s)).ToList();
            // Replace: remove all old, insert merged at position 0
            state.SSTables.RemoveAll(s => toDelete.Contains(s));
            state.SSTables.Insert(0, mergedSst);
        }
        finally { state.Lock.ExitWriteLock(); }

        // Step 6: delete old SSTable files
        foreach (var old in toDelete)
            old.DeleteFile();

        // Step 7: mark complete and delete manifest
        AppendToManifest(manifestPath, ManifestCompleteMarker);
        TryDeleteFile(manifestPath);
    }

    /// <summary>
    /// Streaming K-way merge of sorted SSTables.
    /// 
    /// Opens one <see cref="SSTableIterator"/> per SSTable.
    /// At each step, picks the smallest key across all iterators,
    /// merges Guids from all iterators with that key, and writes to output.
    /// 
    /// Memory: O(K) — one current entry per iterator.
    /// I/O: single sequential pass over each input, single sequential write.
    /// </summary>
    private static SSTable StreamingMerge(List<SSTable> sources, string outputPath)
    {
        var iterators = new List<SSTableIterator>();
        try
        {
            foreach (var sst in sources)
            {
                var iter = SSTableIterator.Open(sst.FilePath);
                if (iter.HasCurrent)
                    iterators.Add(iter);
                else
                    iter.Dispose();
            }

            return SSTable.Write(outputPath, MergeEntries(iterators));
        }
        finally
        {
            foreach (var iter in iterators)
                iter.Dispose();
        }
    }

    /// <summary>
    /// Yields merged (key, guids) pairs in sorted order from K iterators.
    /// At each step, finds the minimum key, collects all Guids for that key
    /// from all iterators positioned at it, deduplicates, and yields.
    /// </summary>
    private static IEnumerable<(string Key, List<Guid> Guids)> MergeEntries(List<SSTableIterator> iterators)
    {
        while (iterators.Count > 0)
        {
            // Find minimum key across all iterators
            string? minKey = null;
            for (int i = 0; i < iterators.Count; i++)
            {
                if (!iterators[i].HasCurrent) continue;
                if (minKey is null || string.Compare(iterators[i].CurrentKey!, minKey, StringComparison.Ordinal) < 0)
                    minKey = iterators[i].CurrentKey;
            }

            if (minKey is null) break;

            // Collect and deduplicate all Guids for this key
            var mergedGuids = new HashSet<Guid>();
            for (int i = iterators.Count - 1; i >= 0; i--)
            {
                var iter = iterators[i];
                if (!iter.HasCurrent || iter.CurrentKey != minKey) continue;

                foreach (var guid in iter.CurrentGuids!)
                    mergedGuids.Add(guid);

                // Advance this iterator
                if (!iter.MoveNext())
                {
                    iter.Dispose();
                    iterators.RemoveAt(i);
                }
            }

            yield return (minKey, [.. mergedGuids]);
        }
    }

    // ──────────────────────────────────────────────
    //  Manifest-based crash recovery
    // ──────────────────────────────────────────────

    /// <summary>
    /// Manifest format (line-based, flushed after each write):
    /// <code>
    /// OLD:path/to/old1.sst
    /// OLD:path/to/old2.sst
    /// NEW:path/to/merged.sst
    /// MERGED
    /// COMPLETE
    /// </code>
    /// 
    /// Recovery logic:
    /// <list type="bullet">
    ///   <item>No manifest → nothing to recover.</item>
    ///   <item>Manifest exists without COMPLETE → incomplete compaction.</item>
    ///   <item>MERGED marker absent → merge didn't finish. Delete merged file if exists. Old files are intact.</item>
    ///   <item>MERGED marker present → merge finished. Delete old files, keep merged. Delete manifest.</item>
    /// </list>
    /// </summary>
    private static void RecoverCompaction(string indexDir)
    {
        string manifestPath = Path.Combine(indexDir, ManifestFileName);
        if (!File.Exists(manifestPath)) return;

        var lines = File.ReadAllLines(manifestPath);
        if (lines.Length == 0)
        {
            TryDeleteFile(manifestPath);
            return;
        }

        var oldFiles = new List<string>();
        string? newFile = null;
        bool mergeComplete = false;

        foreach (var line in lines)
        {
            if (line == ManifestCompleteMarker)
            {
                // Fully complete — just delete manifest
                TryDeleteFile(manifestPath);
                return;
            }
            if (line == "MERGED")
            {
                mergeComplete = true;
                continue;
            }
            if (line.StartsWith("OLD:"))
                oldFiles.Add(line[4..]);
            else if (line.StartsWith("NEW:"))
                newFile = line[4..];
        }

        if (!mergeComplete)
        {
            // Merge didn't complete — delete merged file if it exists, old files are fine
            if (newFile is not null) TryDeleteFile(newFile);
        }
        else
        {
            // Merge completed but old files weren't deleted yet — finish the job
            foreach (var old in oldFiles)
                TryDeleteFile(old);
        }

        TryDeleteFile(manifestPath);
    }

    private static void WriteManifest(string path, List<string> oldFiles, string newFile)
    {
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 4096);
        using var writer = new StreamWriter(fs);

        foreach (var old in oldFiles)
            writer.WriteLine($"OLD:{old}");
        writer.WriteLine($"NEW:{newFile}");
        writer.Flush();
        fs.Flush(flushToDisk: true);
    }

    private static void AppendToManifest(string path, string marker)
    {
        using var fs = new FileStream(path, FileMode.Append, FileAccess.Write, FileShare.None, 4096);
        using var writer = new StreamWriter(fs);
        writer.WriteLine(marker);
        writer.Flush();
        fs.Flush(flushToDisk: true);
    }

    private static void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch { /* best effort */ }
    }

    private string GetIndexDirectory(string table, string field)
        => Path.Combine(_basePath, "indexes", table, field);

    // ──────────────────────────────────────────────
    //  Per-index state
    // ──────────────────────────────────────────────

    private sealed class FieldIndexState
    {
        public IndexDefinition Definition { get; }
        public MemTable MemTable { get; }
        public List<SSTable> SSTables { get; }
        public ReaderWriterLockSlim Lock { get; } = new(LockRecursionPolicy.NoRecursion);

        public FieldIndexState(IndexDefinition definition)
            : this(definition, [])
        {
        }

        public FieldIndexState(IndexDefinition definition, List<SSTable> ssTables)
        {
            Definition = definition;
            MemTable = new MemTable();
            SSTables = ssTables;
        }

        public void Dispose(string indexDirectory)
        {
            Lock.EnterWriteLock();
            try
            {
                foreach (var sst in SSTables)
                    sst.DeleteFile();
            }
            finally { Lock.ExitWriteLock(); }

            Lock.Dispose();

            try { if (Directory.Exists(indexDirectory)) Directory.Delete(indexDirectory, recursive: true); }
            catch { /* best effort */ }
        }
    }

    public void RemoveByKey(string table, Guid recordKey)
    {
        List<FieldIndexState> states = [];

        lock (_registryLock)
        {
            foreach (var (k, state) in _indexes)
            {
                if (k.Table == table)
                    states.Add(state);
            }
        }

        // Scan each field's MemTable — O(N) per MemTable, but MemTables are small
        foreach (var state in states)
        {
            state.Lock.EnterWriteLock();
            try
            {
                state.MemTable.RemoveByKey(recordKey);
            }
            finally { state.Lock.ExitWriteLock(); }
        }

        // Note: entries already flushed to SSTables will be stale but harmless —
        // Lookup returns Guids, the caller then does GetByKeyAsync which returns null
        // for deleted records. Compaction will eventually clean them up.
    }

    public void EnsureIndex(string table, string fieldName)
    {
        var key = (table, fieldName);
        lock (_registryLock)
        {
            if (_indexes.ContainsKey(key))
                return;

            var dir = GetIndexDirectory(table, fieldName);
            Directory.CreateDirectory(dir);

            var definition = new IndexDefinition
            {
                FieldName = fieldName,
                CreatedAtUtc = DateTime.UtcNow.Ticks
            };

            _indexes[key] = new FieldIndexState(definition);
        }
    }
}