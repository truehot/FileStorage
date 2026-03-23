using FileStorage.Infrastructure.Checkpoint;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.IO;
using FileStorage.Infrastructure.Recovery;
using FileStorage.Infrastructure.WAL;
using Moq;

namespace FileStorage.Infrastructure.Tests;

public class StorageEngineTests
{
    // Test: SaveAsync save in WAL and call checkpoint
    [Fact]
    public async Task SaveAsync_AppendsToWal_AndTracksCheckpoint()
    {
        var mockIndex = new Mock<IMmapRegion>();
        var mockData = new Mock<IMmapRegion>();
        var mockWal = new Mock<IWriteAheadLog>();
        var mockCheckpoint = new Mock<ICheckpointManager>();
        var mockRecovery = new Mock<IStorageRecovery>();

        // Mock WAL ReadAll to return an empty list (no entries to replay)
        mockWal.Setup(w => w.ReadAll()).Returns(new List<WalEntry>());

        mockRecovery.Setup(r => r.Initialize(
            It.IsAny<IMmapRegion>(), It.IsAny<IMmapRegion>(),
            It.IsAny<IWriteAheadLog>(), It.IsAny<IMemoryIndex>(),
            It.IsAny<IIndexManager>()))
            .Returns(new StorageRecovery.RecoveryResult(4096, 0));

        var regions = new RegionProvider(mockIndex.Object, mockData.Object);

        using var engine = new StorageEngine(
            regions, mockWal.Object,
            checkpoint: mockCheckpoint.Object,
            recovery: mockRecovery.Object);

        await engine.InitializeAsync();
        await engine.SaveAsync("users", Guid.NewGuid(), [1, 2, 3]);

        mockWal.Verify(w => w.Append(It.IsAny<WalEntry>()), Times.Once);
        mockCheckpoint.Verify(c => c.TrackWrite(), Times.Once);
    }
}

