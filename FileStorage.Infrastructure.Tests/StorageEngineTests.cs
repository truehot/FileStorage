using FileStorage.Infrastructure.Checkpoint;
using FileStorage.Infrastructure.Compaction;
using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Core.Lifecycle;
using FileStorage.Infrastructure.Core.Models;
using FileStorage.Infrastructure.Core.Operations;
using FileStorage.Infrastructure.Core.Serialization;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Indexing.SecondaryIndex;
using FileStorage.Infrastructure.Recovery;
using FileStorage.Infrastructure.WAL;
using Moq;

namespace FileStorage.Infrastructure.Tests;

public class StorageEngineTests
{
    [Fact]
    public async Task SaveAsync_AppendsToWal_AndTracksCheckpoint()
    {
        var mockIndexRegion = new Mock<IMmapRegion>();
        var mockDataRegion = new Mock<IMmapRegion>();
        var mockWal = new Mock<IWriteAheadLog>();
        var mockCheckpoint = new Mock<ICheckpointManager>();
        var mockRecovery = new Mock<IStorageRecovery>();
        var mockIndexManager = new Mock<IIndexManager>();
        var mockSecondary = new Mock<ISecondaryIndexManager>();
        var mockReader = new Mock<IRecordReader>();

        mockWal.Setup(w => w.ReadAll()).Returns([]);
        mockWal.Setup(w => w.ReadAllStreaming()).Returns([]);

        mockRecovery.Setup(r => r.Initialize(
            It.IsAny<IMmapRegion>(), It.IsAny<IMmapRegion>(),
            It.IsAny<IWriteAheadLog>(), It.IsAny<IMemoryIndex>(),
            It.IsAny<IIndexManager>()))
            .Returns(new StorageRecovery.RecoveryResult(4096, 0));

        IRegionProvider regions = new RegionProvider(mockIndexRegion.Object, mockDataRegion.Object);
        IMemoryIndex memoryIndex = new MemoryIndex();
        ICompactionService compaction = new CompactionService();

        using var engine = CreateEngine(
            regions,
            mockWal.Object,
            memoryIndex,
            mockIndexManager.Object,
            mockReader.Object,
            mockCheckpoint.Object,
            mockRecovery.Object,
            compaction,
            mockSecondary.Object);

        await engine.InitializeAsync();
        await engine.SaveAsync("users", Guid.NewGuid(), [1, 2, 3]);

        mockWal.Verify(w => w.Append(It.IsAny<WalEntry>()), Times.Once);
        mockCheckpoint.Verify(c => c.TrackWrite(), Times.Once);
    }

    [Fact]
    public async Task SaveBatchAsync_AppendsSingleWalEntry_AndUsesBulkSecondaryIndex()
    {
        var mockIndexRegion = new Mock<IMmapRegion>();
        var mockDataRegion = new Mock<IMmapRegion>();
        var mockWal = new Mock<IWriteAheadLog>();
        var mockCheckpoint = new Mock<ICheckpointManager>();
        var mockRecovery = new Mock<IStorageRecovery>();
        var mockIndexManager = new Mock<IIndexManager>();
        var mockSecondary = new Mock<ISecondaryIndexManager>();
        var mockReader = new Mock<IRecordReader>();

        mockWal.Setup(w => w.ReadAll()).Returns([]);
        mockWal.Setup(w => w.ReadAllStreaming()).Returns([]);

        mockRecovery.Setup(r => r.Initialize(
            It.IsAny<IMmapRegion>(), It.IsAny<IMmapRegion>(),
            It.IsAny<IWriteAheadLog>(), It.IsAny<IMemoryIndex>(),
            It.IsAny<IIndexManager>()))
            .Returns(new StorageRecovery.RecoveryResult(4096, 0));

        mockIndexManager.SetupGet(i => i.NextDataOffset).Returns(100);
        mockIndexManager.SetupGet(i => i.NextIndexOffset).Returns(4096);
        mockIndexManager.SetupGet(i => i.EntrySize).Returns(301);

        IRegionProvider regions = new RegionProvider(mockIndexRegion.Object, mockDataRegion.Object);
        IMemoryIndex memoryIndex = new MemoryIndex();
        ICompactionService compaction = new CompactionService();

        using var engine = CreateEngine(
            regions,
            mockWal.Object,
            memoryIndex,
            mockIndexManager.Object,
            mockReader.Object,
            mockCheckpoint.Object,
            mockRecovery.Object,
            compaction,
            mockSecondary.Object);

        await engine.InitializeAsync();

        var entries = new List<StorageWriteEntry>
        {
            new(Guid.NewGuid(), [1,2,3], new Dictionary<string, string> { ["status"] = "active" }),
            new(Guid.NewGuid(), [4,5], new Dictionary<string, string> { ["status"] = "inactive" })
        };

        await engine.SaveBatchAsync("users", entries);

        mockWal.Verify(w => w.Append(It.Is<WalEntry>(e => e.Operation == WalOperationType.SaveBatch)), Times.Once);
        mockIndexManager.Verify(i => i.ApplySavePhysical(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<byte[]>(), It.IsAny<long>(), It.IsAny<long>()), Times.Exactly(2));
        mockIndexManager.Verify(i => i.PublishSave(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<long>()), Times.Exactly(2));
        mockSecondary.Verify(s => s.PutRange("users", It.Is<IReadOnlyCollection<(Guid RecordKey, IReadOnlyDictionary<string, string> IndexedFields)>>(b => b.Count == 2)), Times.Once);
        mockCheckpoint.Verify(c => c.TrackWrite(), Times.Once);
    }

    [Fact]
    public async Task SaveAsync_UsesWalFirst_Order()
    {
        var mockIndexRegion = new Mock<IMmapRegion>();
        var mockDataRegion = new Mock<IMmapRegion>();
        var mockWal = new Mock<IWriteAheadLog>();
        var mockCheckpoint = new Mock<ICheckpointManager>();
        var mockRecovery = new Mock<IStorageRecovery>();
        var mockIndexManager = new Mock<IIndexManager>();
        var mockSecondary = new Mock<ISecondaryIndexManager>();
        var mockReader = new Mock<IRecordReader>();

        mockWal.Setup(w => w.ReadAll()).Returns([]);
        mockWal.Setup(w => w.ReadAllStreaming()).Returns([]);

        mockRecovery.Setup(r => r.Initialize(
            It.IsAny<IMmapRegion>(), It.IsAny<IMmapRegion>(),
            It.IsAny<IWriteAheadLog>(), It.IsAny<IMemoryIndex>(),
            It.IsAny<IIndexManager>()))
            .Returns(new StorageRecovery.RecoveryResult(4096, 0));

        mockIndexManager.SetupGet(i => i.NextDataOffset).Returns(128);
        mockIndexManager.SetupGet(i => i.NextIndexOffset).Returns(8192);

        var callOrder = new List<string>();

        mockWal
            .Setup(w => w.Append(It.Is<WalEntry>(e =>
                e.Operation == WalOperationType.Save &&
                e.Table == "users" &&
                e.DataOffset == 128 &&
                e.IndexOffset == 8192 &&
                e.Data.Length == 3 &&
                e.Data[0] == 1 &&
                e.Data[1] == 2 &&
                e.Data[2] == 3)))
            .Callback(() => callOrder.Add("wal"))
            .Returns(1);

        mockIndexManager
            .Setup(i => i.ApplySavePhysical("users", It.IsAny<Guid>(), It.IsAny<byte[]>(), 128, 8192))
            .Callback(() => callOrder.Add("physical"));

        mockIndexManager
            .Setup(i => i.PublishSave("users", It.IsAny<Guid>(), 8192))
            .Callback(() => callOrder.Add("publish"));

        IRegionProvider regions = new RegionProvider(mockIndexRegion.Object, mockDataRegion.Object);
        IMemoryIndex memoryIndex = new MemoryIndex();
        ICompactionService compaction = new CompactionService();

        using var engine = CreateEngine(
            regions,
            mockWal.Object,
            memoryIndex,
            mockIndexManager.Object,
            mockReader.Object,
            mockCheckpoint.Object,
            mockRecovery.Object,
            compaction,
            mockSecondary.Object);

        await engine.InitializeAsync();
        await engine.SaveAsync("users", Guid.NewGuid(), [1, 2, 3]);

        Assert.Equal(["wal", "physical", "publish"], callOrder);
        mockCheckpoint.Verify(c => c.TrackWrite(), Times.Once);
    }

    [Fact]
    public async Task SaveAsync_WithIndexedFields_AppendsSingleEntrySaveBatchPayload()
    {
        var mockIndexRegion = new Mock<IMmapRegion>();
        var mockDataRegion = new Mock<IMmapRegion>();
        var mockWal = new Mock<IWriteAheadLog>();
        var mockCheckpoint = new Mock<ICheckpointManager>();
        var mockRecovery = new Mock<IStorageRecovery>();
        var mockIndexManager = new Mock<IIndexManager>();
        var mockSecondary = new Mock<ISecondaryIndexManager>();
        var mockReader = new Mock<IRecordReader>();

        mockWal.Setup(w => w.ReadAll()).Returns([]);
        mockWal.Setup(w => w.ReadAllStreaming()).Returns([]);

        mockRecovery.Setup(r => r.Initialize(
            It.IsAny<IMmapRegion>(), It.IsAny<IMmapRegion>(),
            It.IsAny<IWriteAheadLog>(), It.IsAny<IMemoryIndex>(),
            It.IsAny<IIndexManager>()))
            .Returns(new StorageRecovery.RecoveryResult(4096, 0));

        const long dataOffset = 100;
        const long indexOffset = 4096;
        mockIndexManager.SetupGet(i => i.NextDataOffset).Returns(dataOffset);
        mockIndexManager.SetupGet(i => i.NextIndexOffset).Returns(indexOffset);

        IRegionProvider regions = new RegionProvider(mockIndexRegion.Object, mockDataRegion.Object);
        IMemoryIndex memoryIndex = new MemoryIndex();
        ICompactionService compaction = new CompactionService();

        using var engine = CreateEngine(
            regions,
            mockWal.Object,
            memoryIndex,
            mockIndexManager.Object,
            mockReader.Object,
            mockCheckpoint.Object,
            mockRecovery.Object,
            compaction,
            mockSecondary.Object);

        await engine.InitializeAsync();

        var key = Guid.NewGuid();
        byte[] payload = [1, 2, 3, 4];
        var indexedFields = new Dictionary<string, string> { ["status"] = "active" };

        await engine.SaveAsync("users", key, payload, indexedFields);

        mockWal.Verify(w => w.Append(It.Is<WalEntry>(entry =>
            IsSingleEntrySaveBatch(entry, "users", key, payload, dataOffset, indexOffset, indexedFields))), Times.Once);

        mockSecondary.Verify(s => s.Put("users", key, indexedFields), Times.Once);
        mockCheckpoint.Verify(c => c.TrackWrite(), Times.Once);
    }

    [Fact]
    public async Task InitializeAsync_ReplaysSecondaryBeforeWalCheckpoint()
    {
        var mockIndexRegion = new Mock<IMmapRegion>();
        var mockDataRegion = new Mock<IMmapRegion>();
        var mockWal = new Mock<IWriteAheadLog>();
        var mockCheckpoint = new Mock<ICheckpointManager>();
        var mockRecovery = new Mock<IStorageRecovery>();
        var mockIndexManager = new Mock<IIndexManager>();
        var mockSecondary = new Mock<ISecondaryIndexManager>();
        var mockReader = new Mock<IRecordReader>();

        var callOrder = new List<string>();

        mockRecovery.Setup(r => r.Initialize(
            It.IsAny<IMmapRegion>(), It.IsAny<IMmapRegion>(),
            It.IsAny<IWriteAheadLog>(), It.IsAny<IMemoryIndex>(),
            It.IsAny<IIndexManager>()))
            .Callback(() => callOrder.Add("recovery"))
            .Returns(new StorageRecovery.RecoveryResult(4096, 0));

        var key = Guid.NewGuid();
        var indexedFields = new Dictionary<string, string> { ["status"] = "active" };
        byte[] payload = WalBatchPayloadSerializer.Serialize([
            new WalBatchEntry(key, [10, 11], 0, 4096, indexedFields)
        ]);

        mockWal.Setup(w => w.ReadAllStreaming()).Returns([
            new WalEntry
            {
                Operation = WalOperationType.SaveBatch,
                Table = "users",
                Key = Guid.Empty,
                Data = payload,
                DataOffset = 0,
                IndexOffset = 4096,
                IndexedFields = new Dictionary<string, string>()
            }
        ]);

        mockWal.Setup(w => w.Checkpoint()).Callback(() => callOrder.Add("checkpoint"));
        mockSecondary.Setup(s => s.LoadExisting()).Callback(() => callOrder.Add("loadexisting"));
        mockSecondary.Setup(s => s.PutRange("users", It.IsAny<IReadOnlyCollection<(Guid RecordKey, IReadOnlyDictionary<string, string> IndexedFields)>>()))
            .Callback(() => callOrder.Add("replay"));

        IRegionProvider regions = new RegionProvider(mockIndexRegion.Object, mockDataRegion.Object);
        IMemoryIndex memoryIndex = new MemoryIndex();
        ICompactionService compaction = new CompactionService();

        using var engine = CreateEngine(
            regions,
            mockWal.Object,
            memoryIndex,
            mockIndexManager.Object,
            mockReader.Object,
            mockCheckpoint.Object,
            mockRecovery.Object,
            compaction,
            mockSecondary.Object);

        await engine.InitializeAsync();

        Assert.Equal(["recovery", "loadexisting", "replay", "checkpoint"], callOrder);
    }

    private static bool IsSingleEntrySaveBatch(
        WalEntry entry,
        string expectedTable,
        Guid expectedKey,
        byte[] expectedData,
        long expectedDataOffset,
        long expectedIndexOffset,
        Dictionary<string, string> expectedIndexedFields)
    {
        if (entry.Operation != WalOperationType.SaveBatch)
            return false;

        if (!string.Equals(entry.Table, expectedTable, StringComparison.Ordinal))
            return false;

        if (entry.DataOffset != expectedDataOffset || entry.IndexOffset != expectedIndexOffset)
            return false;

        if (!WalBatchPayloadSerializer.TryDeserialize(entry.Data, out var batchEntries))
            return false;

        if (batchEntries.Count != 1)
            return false;

        var batchEntry = batchEntries[0];
        if (batchEntry.Key != expectedKey)
            return false;

        if (batchEntry.DataOffset != expectedDataOffset || batchEntry.IndexOffset != expectedIndexOffset)
            return false;

        if (!batchEntry.Data.AsSpan().SequenceEqual(expectedData))
            return false;

        if (batchEntry.IndexedFields.Count != expectedIndexedFields.Count)
            return false;

        foreach (var (field, value) in expectedIndexedFields)
        {
            if (!batchEntry.IndexedFields.TryGetValue(field, out var actualValue))
                return false;

            if (!string.Equals(actualValue, value, StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    private static StorageEngine CreateEngine(
        IRegionProvider regions,
        IWriteAheadLog wal,
        IMemoryIndex memoryIndex,
        IIndexManager indexManager,
        IRecordReader recordReader,
        ICheckpointManager checkpoint,
        IStorageRecovery recovery,
        ICompactionService compaction,
        FileStorage.Infrastructure.Indexing.SecondaryIndex.ISecondaryIndexManager secondaryIndex)
    {
        var checkpointHandle = new CheckpointHandle(checkpoint);
        var lifetime = new StorageEngineLifetime();
        var secondaryIndexReplayService = new SecondaryIndexReplayService(wal, secondaryIndex);
        var startupOperations = new StorageStartupOperations(
            regions,
            wal,
            memoryIndex,
            indexManager,
            recovery,
            secondaryIndex,
            secondaryIndexReplayService);
        var readOperations = new StorageReadOperations(regions, memoryIndex, indexManager, recordReader);
        var writeOperations = new StorageWriteOperations(wal, memoryIndex, indexManager, secondaryIndex, checkpointHandle);
        var indexOperations = new StorageIndexOperations(secondaryIndex);
        var maintenanceOperations = new StorageMaintenanceOperations(
            regions,
            memoryIndex,
            indexManager,
            wal,
            compaction,
            checkpointHandle);

        return new StorageEngine(
            regions,
            wal,
            secondaryIndex,
            checkpointHandle,
            lifetime,
            startupOperations,
            readOperations,
            writeOperations,
            indexOperations,
            maintenanceOperations);
    }
}

