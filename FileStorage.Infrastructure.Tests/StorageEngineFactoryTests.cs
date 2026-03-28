using FileStorage.Abstractions.SecondaryIndex;
using FileStorage.Infrastructure.Checkpoint;
using FileStorage.Infrastructure.Compaction;
using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Core.Serialization;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Indexing.SecondaryIndex;
using FileStorage.Infrastructure.Recovery;
using FileStorage.Infrastructure.WAL;
using Moq;

namespace FileStorage.Infrastructure.Tests;

public sealed class StorageEngineFactoryTests
{
    [Fact]
    public async Task CreateAsync_WithInjectedDependencies_WhenInitializationFails_DisposesOwnedResources()
    {
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();

        var regions = new Mock<IRegionProvider>();
        regions.SetupGet(r => r.IndexRegion).Returns(indexRegion.Object);
        regions.SetupGet(r => r.DataRegion).Returns(dataRegion.Object);

        var wal = new Mock<IWriteAheadLog>();
        wal.SetupGet(w => w.SequenceNumber).Returns(0L);

        var memoryIndex = new Mock<IMemoryIndex>();
        var indexManager = new Mock<IIndexManager>();
        var recordReader = new Mock<IRecordReader>();
        var checkpoint = new Mock<ICheckpointManager>();
        var recovery = new Mock<IStorageRecovery>();
        var compaction = new Mock<ICompactionService>();
        var secondaryIndex = new DisposableSecondaryIndexManager();

        recovery
            .Setup(r => r.Initialize(
                It.IsAny<IMmapRegion>(),
                It.IsAny<IMmapRegion>(),
                It.IsAny<IWriteAheadLog>(),
                It.IsAny<IMemoryIndex>(),
                It.IsAny<IIndexManager>()))
            .Throws(new InvalidOperationException("init failed"));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            StorageEngineFactory.CreateAsync(
                regions.Object,
                wal.Object,
                memoryIndex.Object,
                indexManager.Object,
                recordReader.Object,
                checkpoint.Object,
                recovery.Object,
                compaction.Object,
                secondaryIndex));

        wal.Verify(w => w.Dispose(), Times.Once);
        regions.Verify(r => r.Dispose(), Times.Once);
        Assert.True(secondaryIndex.IsDisposed);
    }

    private sealed class DisposableSecondaryIndexManager : ISecondaryIndexManager, IDisposable
    {
        public bool IsDisposed { get; private set; }

        public void Dispose() => IsDisposed = true;

        public void DropIndex(string table, string fieldName) { }

        public IReadOnlyList<IndexDefinition> GetIndexes(string table) => [];

        public bool HasIndex(string table, string fieldName) => false;

        public void Put(string table, Guid recordKey, IReadOnlyDictionary<string, string> indexedFields) { }

        public void PutRange(string table, IReadOnlyCollection<(Guid RecordKey, IReadOnlyDictionary<string, string> IndexedFields)> entries) { }

        public void Remove(string table, Guid recordKey, IReadOnlyDictionary<string, string> previousValues) { }

        public void RemoveByKey(string table, Guid recordKey) { }

        public List<Guid> Lookup(string table, string fieldName, string value) => [];

        public void DropAllIndexes(string table) { }

        public void EnsureIndex(string table, string fieldName) { }

        public void LoadExisting() { }
    }
}
