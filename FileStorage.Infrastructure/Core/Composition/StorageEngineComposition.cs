using FileStorage.Infrastructure.Checkpoint;
using FileStorage.Infrastructure.Compaction;
using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Core.Lifecycle;
using FileStorage.Infrastructure.Core.Operations;
using FileStorage.Infrastructure.Core.Serialization;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Indexing.SecondaryIndex;
using FileStorage.Infrastructure.Recovery;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure.Core.Composition;

/// <summary>
/// Composes all non-index engine services.
/// Responsible for: record reader, checkpoint, recovery, compaction, lifetime, startup, and operations.
/// </summary>
internal static class StorageEngineComposition
{
    public static (
        CheckpointHandle CheckpointHandle,
        StorageEngineLifetime Lifetime,
        StorageStartupOperations Startup,
        StorageReadOperations ReadOperations,
        StorageWriteOperations WriteOperations,
        StorageIndexOperations IndexOperations,
        StorageMaintenanceOperations MaintenanceOperations
    ) CreateEngineServices(
        IRegionProvider regions,
        IWriteAheadLog wal,
        IMemoryIndex memoryIndex,
        IIndexManager indexManager,
        ISecondaryIndexManager secondaryIndex,
        int checkpointThreshold,
        int readParallelism)
    {
        ArgumentNullException.ThrowIfNull(regions);
        ArgumentNullException.ThrowIfNull(wal);
        ArgumentNullException.ThrowIfNull(memoryIndex);
        ArgumentNullException.ThrowIfNull(indexManager);
        ArgumentNullException.ThrowIfNull(secondaryIndex);

        IRecordReader recordReader = new RecordReader();
        var checkpointHandle = new CheckpointHandle(new CheckpointManager(regions.IndexRegion, regions.DataRegion, wal, checkpointThreshold));
        IStorageRecovery recovery = new StorageRecovery();
        ICompactionService compaction = new CompactionService();
        var lifetime = new StorageEngineLifetime();
        var secondaryIndexReplayService = new SecondaryIndexReplayService(wal, secondaryIndex);
        var startup = new StorageStartupOperations(
            regions,
            wal,
            memoryIndex,
            indexManager,
            recovery,
            secondaryIndex,
            secondaryIndexReplayService);
        var readOperations = new StorageReadOperations(regions, memoryIndex, indexManager, recordReader, readParallelism);
        var writeOperations = new StorageWriteOperations(wal, memoryIndex, indexManager, secondaryIndex, checkpointHandle);
        var indexOperations = new StorageIndexOperations(secondaryIndex);
        var maintenanceOperations = new StorageMaintenanceOperations(
            regions,
            memoryIndex,
            indexManager,
            wal,
            compaction,
            checkpointHandle,
            checkpointThreshold);

        return (
            checkpointHandle,
            lifetime,
            startup,
            readOperations,
            writeOperations,
            indexOperations,
            maintenanceOperations);
    }
}
