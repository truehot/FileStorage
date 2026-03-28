using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Core.Serialization;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Recovery;
using FileStorage.Infrastructure.WAL;
using Moq;

namespace FileStorage.Infrastructure.Tests.Recovery;

public sealed class StorageRecoveryBatchTests
{
    [Fact]
    public void Initialize_ReplaysSave_WithExactOffsets()
    {
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        var memoryIndex = new Mock<IMemoryIndex>();
        var indexManager = new Mock<IIndexManager>();

        long nextIndexOffset = 0;
        long nextDataOffset = 0;

        indexManager.SetupGet(i => i.NextIndexOffset).Returns(() => nextIndexOffset);
        indexManager.SetupGet(i => i.NextDataOffset).Returns(() => nextDataOffset);
        indexManager
            .Setup(i => i.SetWritePositions(It.IsAny<long>(), It.IsAny<long>()))
            .Callback<long, long>((indexPos, dataPos) =>
            {
                nextIndexOffset = indexPos;
                nextDataOffset = dataPos;
            });

        indexRegion.SetupGet(r => r.Path).Returns("index.idx");
        dataRegion.SetupGet(r => r.Path).Returns("data.dat");

        indexRegion.Setup(r => r.Read(0, It.IsAny<byte[]>(), 0, 4))
            .Callback<long, byte[], int, int>((_, b, _, _) => { b[0] = 0; b[1] = 0; b[2] = 0; b[3] = 0; });

        indexRegion.SetupGet(r => r.FileSize).Returns(1024 * 1024);
        dataRegion.SetupGet(r => r.FileSize).Returns(1024 * 1024);

        var key = Guid.NewGuid();

        wal.Setup(w => w.ReadAllStreaming()).Returns([
            new WalEntry
            {
                Operation = WalOperationType.Save,
                Table = "users",
                Key = key,
                Data = [1, 2, 3, 4],
                DataOffset = 0,
                IndexOffset = 4096,
                IndexedFields = new Dictionary<string, string>()
            }
        ]);

        var recovery = new StorageRecovery();
        recovery.Initialize(indexRegion.Object, dataRegion.Object, wal.Object, memoryIndex.Object, indexManager.Object);

        indexManager.Verify(i => i.ApplySave("users", key, It.Is<byte[]>(d => d.Length == 4), 0, 4096), Times.Once);
        indexRegion.Verify(r => r.Flush(), Times.Once);
        dataRegion.Verify(r => r.Flush(), Times.Once);
        wal.Verify(w => w.Checkpoint(), Times.Never);
    }

    [Fact]
    public void Initialize_ReplaysSaveBatch_WithExactOffsets()
    {
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        var memoryIndex = new Mock<IMemoryIndex>();
        var indexManager = new Mock<IIndexManager>();

        long nextIndexOffset = 0;
        long nextDataOffset = 0;

        indexManager.SetupGet(i => i.NextIndexOffset).Returns(() => nextIndexOffset);
        indexManager.SetupGet(i => i.NextDataOffset).Returns(() => nextDataOffset);
        indexManager
            .Setup(i => i.SetWritePositions(It.IsAny<long>(), It.IsAny<long>()))
            .Callback<long, long>((indexPos, dataPos) =>
            {
                nextIndexOffset = indexPos;
                nextDataOffset = dataPos;
            });

        indexRegion.SetupGet(r => r.Path).Returns("index.idx");
        dataRegion.SetupGet(r => r.Path).Returns("data.dat");

        // New DB header path (not existing): header[0] != Magic0
        indexRegion.Setup(r => r.Read(0, It.IsAny<byte[]>(), 0, 4))
            .Callback<long, byte[], int, int>((_, b, _, _) => { b[0] = 0; b[1] = 0; b[2] = 0; b[3] = 0; });

        indexRegion.SetupGet(r => r.FileSize).Returns(1024 * 1024);
        dataRegion.SetupGet(r => r.FileSize).Returns(1024 * 1024);

        var k1 = Guid.NewGuid();
        var k2 = Guid.NewGuid();

        byte[] payload = WalBatchPayloadSerializer.Serialize([
            new WalBatchEntry(k1, [1,2,3], DataOffset: 0, IndexOffset: 4096, new Dictionary<string, string>()),
            new WalBatchEntry(k2, [4,5], DataOffset: 3, IndexOffset: 4096 + IndexEntrySerializer.EntryFixedSize, new Dictionary<string, string>())
        ]);

        wal.Setup(w => w.ReadAllStreaming()).Returns([
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

        var recovery = new StorageRecovery();
        recovery.Initialize(indexRegion.Object, dataRegion.Object, wal.Object, memoryIndex.Object, indexManager.Object);

        indexManager.Verify(i => i.ApplySave("users", k1, It.Is<byte[]>(d => d.Length == 3), 0, 4096), Times.Once);
        indexManager.Verify(i => i.ApplySave("users", k2, It.Is<byte[]>(d => d.Length == 2), 3, 4096 + IndexEntrySerializer.EntryFixedSize), Times.Once);

        indexRegion.Verify(r => r.Flush(), Times.Once);
        dataRegion.Verify(r => r.Flush(), Times.Once);
        wal.Verify(w => w.Checkpoint(), Times.Never);
    }

    [Fact]
    public void Initialize_WhenSaveBatchExceedsCurrentFileSizeButMatchesCursor_ReplaysBatch()
    {
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        var memoryIndex = new Mock<IMemoryIndex>();
        var indexManager = new Mock<IIndexManager>();

        long nextIndexOffset = 0;
        long nextDataOffset = 0;

        indexManager.SetupGet(i => i.NextIndexOffset).Returns(() => nextIndexOffset);
        indexManager.SetupGet(i => i.NextDataOffset).Returns(() => nextDataOffset);
        indexManager
            .Setup(i => i.SetWritePositions(It.IsAny<long>(), It.IsAny<long>()))
            .Callback<long, long>((indexPos, dataPos) =>
            {
                nextIndexOffset = indexPos;
                nextDataOffset = dataPos;
            });

        indexRegion.SetupGet(r => r.Path).Returns("index.idx");
        dataRegion.SetupGet(r => r.Path).Returns("data.dat");

        indexRegion.Setup(r => r.Read(0, It.IsAny<byte[]>(), 0, 4))
            .Callback<long, byte[], int, int>((_, b, _, _) => { b[0] = 0; b[1] = 0; b[2] = 0; b[3] = 0; });

        indexRegion.SetupGet(r => r.FileSize).Returns(4096);
        dataRegion.SetupGet(r => r.FileSize).Returns(1);

        var k1 = Guid.NewGuid();
        var k2 = Guid.NewGuid();

        byte[] payload = WalBatchPayloadSerializer.Serialize([
            new WalBatchEntry(k1, [1,2], DataOffset: 0, IndexOffset: 4096, new Dictionary<string, string>()),
            new WalBatchEntry(k2, [3,4], DataOffset: 2, IndexOffset: 4096 + IndexEntrySerializer.EntryFixedSize, new Dictionary<string, string>())
        ]);

        wal.Setup(w => w.ReadAllStreaming()).Returns([
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

        var recovery = new StorageRecovery();
        recovery.Initialize(indexRegion.Object, dataRegion.Object, wal.Object, memoryIndex.Object, indexManager.Object);

        indexManager.Verify(i => i.ApplySave("users", k1, It.Is<byte[]>(d => d.Length == 2), 0, 4096), Times.Once);
        indexManager.Verify(i => i.ApplySave("users", k2, It.Is<byte[]>(d => d.Length == 2), 2, 4096 + IndexEntrySerializer.EntryFixedSize), Times.Once);
        indexRegion.Verify(r => r.Flush(), Times.Once);
        dataRegion.Verify(r => r.Flush(), Times.Once);
        wal.Verify(w => w.Checkpoint(), Times.Never);
    }

    [Fact]
    public void Initialize_WhenDeleteOffsetPointsToDifferentRecord_SkipsDelete()
    {
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        var memoryIndex = new Mock<IMemoryIndex>();
        var indexManager = new Mock<IIndexManager>();

        indexRegion.SetupGet(r => r.Path).Returns("index.idx");
        dataRegion.SetupGet(r => r.Path).Returns("data.dat");

        var walDeleteKey = Guid.NewGuid();
        var actualKeyAtOffset = Guid.NewGuid();
        const long deleteOffset = 4096;

        indexRegion
            .Setup(r => r.Read(It.IsAny<long>(), It.IsAny<byte[]>(), 0, It.IsAny<int>()))
            .Callback<long, byte[], int, int>((offset, buffer, _, count) =>
            {
                Array.Clear(buffer, 0, count);

                if (offset == 0)
                    return;

                if (offset == deleteOffset)
                    IndexEntrySerializer.Write(buffer.AsSpan(0, IndexEntrySerializer.EntryFixedSize), "orders", actualKeyAtOffset, 10, 2, 10);
            });

        indexRegion.SetupGet(r => r.FileSize).Returns(1024 * 1024);
        dataRegion.SetupGet(r => r.FileSize).Returns(1024 * 1024);

        wal.Setup(w => w.ReadAllStreaming()).Returns([
            new WalEntry
            {
                Operation = WalOperationType.Delete,
                Table = "users",
                Key = walDeleteKey,
                Data = [],
                DataOffset = 0,
                IndexOffset = deleteOffset,
                IndexedFields = new Dictionary<string, string>()
            }
        ]);

        var recovery = new StorageRecovery();
        recovery.Initialize(indexRegion.Object, dataRegion.Object, wal.Object, memoryIndex.Object, indexManager.Object);

        indexManager.Verify(i => i.ApplyDelete(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<long>()), Times.Never);
        indexRegion.Verify(r => r.Flush(), Times.Never);
        dataRegion.Verify(r => r.Flush(), Times.Never);
        wal.Verify(w => w.Checkpoint(), Times.Never);
    }
}
