using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Indexing.SecondaryIndex;
using FileStorage.Infrastructure.Recovery;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure.Core.Operations;

/// <summary>
/// Executes startup initialization and recovery under a caller-owned write lock.
/// </summary>
internal sealed class StorageStartupOperations
{
    private readonly IRegionProvider _regions;
    private readonly IWriteAheadLog _wal;
    private readonly IMemoryIndex _memoryIndex;
    private readonly IIndexManager _indexManager;
    private readonly IStorageRecovery _recovery;
    private readonly ISecondaryIndexManager _secondaryIndex;
    private readonly SecondaryIndexReplayService _secondaryIndexReplayService;

    internal StorageStartupOperations(
        IRegionProvider regions,
        IWriteAheadLog wal,
        IMemoryIndex memoryIndex,
        IIndexManager indexManager,
        IStorageRecovery recovery,
        ISecondaryIndexManager secondaryIndex,
        SecondaryIndexReplayService secondaryIndexReplayService)
    {
        ArgumentNullException.ThrowIfNull(regions);
        ArgumentNullException.ThrowIfNull(wal);
        ArgumentNullException.ThrowIfNull(memoryIndex);
        ArgumentNullException.ThrowIfNull(indexManager);
        ArgumentNullException.ThrowIfNull(recovery);
        ArgumentNullException.ThrowIfNull(secondaryIndex);
        ArgumentNullException.ThrowIfNull(secondaryIndexReplayService);

        _regions = regions;
        _wal = wal;
        _memoryIndex = memoryIndex;
        _indexManager = indexManager;
        _recovery = recovery;
        _secondaryIndex = secondaryIndex;
        _secondaryIndexReplayService = secondaryIndexReplayService;
    }

    /// <summary>
    /// Initializes regions, restores primary state, loads secondary indexes, and replays WAL-derived secondary mutations.
    /// </summary>
    public void Initialize()
    {
        var result = _recovery.Initialize(
            _regions.IndexRegion,
            _regions.DataRegion,
            _wal,
            _memoryIndex,
            _indexManager);

        _indexManager.SetWritePositions(result.IndexWritePos, result.DataWritePos);

        _secondaryIndex.LoadExisting();
        _secondaryIndexReplayService.Replay();
        _wal.Checkpoint();
    }
}
