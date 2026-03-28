using FileStorage.Infrastructure.Checkpoint;
using FileStorage.Infrastructure.Compaction;
using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure.Core.Operations;

/// <summary>
/// Executes maintenance operations under a caller-owned write lock.
/// </summary>
internal sealed class StorageMaintenanceOperations
{
    private readonly IRegionProvider _regions;
    private readonly IMemoryIndex _memoryIndex;
    private readonly IIndexManager _indexManager;
    private readonly IWriteAheadLog _wal;
    private readonly ICompactionService _compaction;
    private readonly CheckpointHandle _checkpointHandle;
    private readonly int _checkpointThreshold;

    internal StorageMaintenanceOperations(
        IRegionProvider regions,
        IMemoryIndex memoryIndex,
        IIndexManager indexManager,
        IWriteAheadLog wal,
        ICompactionService compaction,
        CheckpointHandle checkpointHandle,
        int checkpointThreshold = 1000)
    {
        ArgumentNullException.ThrowIfNull(regions);
        ArgumentNullException.ThrowIfNull(memoryIndex);
        ArgumentNullException.ThrowIfNull(indexManager);
        ArgumentNullException.ThrowIfNull(wal);
        ArgumentNullException.ThrowIfNull(compaction);
        ArgumentNullException.ThrowIfNull(checkpointHandle);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(checkpointThreshold);

        _regions = regions;
        _memoryIndex = memoryIndex;
        _indexManager = indexManager;
        _wal = wal;
        _compaction = compaction;
        _checkpointHandle = checkpointHandle;
        _checkpointThreshold = checkpointThreshold;
    }

    /// <summary>
    /// Rewrites storage files to reclaim space for selected tables or for all tables when the input array is empty.
    /// </summary>
    public Task<long> CompactAsync(string[] tables)
    {
        _checkpointHandle.Current.ForceCheckpoint();

        IReadOnlySet<string>? scope = tables.Length > 0
            ? new HashSet<string>(tables, StringComparer.Ordinal)
            : null;

        long removed = _compaction.Compact(
            _regions.IndexRegion,
            _regions.DataRegion,
            _memoryIndex,
            reopenRegion: _regions.Reopen,
            scope);

        _indexManager.RecalculateWritePositions();
        _checkpointHandle.Current = new CheckpointManager(_regions.IndexRegion, _regions.DataRegion, _wal, _checkpointThreshold);

        return Task.FromResult(removed);
    }
}
