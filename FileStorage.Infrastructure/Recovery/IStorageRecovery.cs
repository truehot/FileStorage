using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.IO;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure.Recovery;

internal interface IStorageRecovery
{
    /// <summary>
    /// Recovers from interrupted compaction (orphaned .tmp files),
    /// loads existing index, and replays WAL.
    /// </summary>
    StorageRecovery.RecoveryResult Initialize(
        IMmapRegion indexRegion,
        IMmapRegion dataRegion,
        IWriteAheadLog wal,
        IMemoryIndex memoryIndex,
        IIndexManager indexManager);
}