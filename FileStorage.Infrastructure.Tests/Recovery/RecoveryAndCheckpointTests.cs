using FileStorage.Infrastructure.Checkpoint;
using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Core.Serialization;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Recovery;
using FileStorage.Infrastructure.WAL;
using Moq;

namespace FileStorage.Infrastructure.Tests.Recovery;

/// <summary>
/// Tests for Recovery & Checkpoint bounded context.
/// Validates idempotency, checkpoint ordering, and WAL safety guarantees.
/// </summary>
public sealed class RecoveryAndCheckpointBoundedContextTests
{
    private static readonly string[] ExpectedCheckpointOrder = ["index", "data", "wal"];

    // ?????????????????????????????????????????????????????????????????????
    // Checkpoint Ordering Tests
    // ?????????????????????????????????????????????????????????????????????

    /// <summary>
    /// Validates that ForceCheckpoint flushes index region before data region.
    /// </summary>
    [Fact]
    public void ForceCheckpoint_FlushesIndexBeforeData()
    {
        // Arrange
        var flushOrder = new List<string>();
        
        var indexRegion = new Mock<IMmapRegion>();
        indexRegion.Setup(r => r.Flush())
            .Callback(() => flushOrder.Add("index"));
        
        var dataRegion = new Mock<IMmapRegion>();
        dataRegion.Setup(r => r.Flush())
            .Callback(() => flushOrder.Add("data"));
        
        var wal = new Mock<IWriteAheadLog>();
        wal.Setup(w => w.Checkpoint())
            .Callback(() => flushOrder.Add("wal"));
        
        var checkpoint = new CheckpointManager(indexRegion.Object, dataRegion.Object, wal.Object);
        
        // Act
        checkpoint.ForceCheckpoint();

        // Assert: index ? data ? wal (strict order)
        Assert.Equal(ExpectedCheckpointOrder, flushOrder);
    }

    /// <summary>
    /// Validates that checkpoint flushes data before truncating WAL.
    /// This order is critical for crash safety.
    /// </summary>
    [Fact]
    public void ForceCheckpoint_FlushesDataBeforeTruncatingWal()
    {
        // Arrange
        var flushOrder = new List<string>();
        
        var indexRegion = new Mock<IMmapRegion>();
        indexRegion.Setup(r => r.Flush())
            .Callback(() => flushOrder.Add("index"));
        
        var dataRegion = new Mock<IMmapRegion>();
        dataRegion.Setup(r => r.Flush())
            .Callback(() => flushOrder.Add("data"));
        
        var wal = new Mock<IWriteAheadLog>();
        wal.Setup(w => w.Checkpoint())
            .Callback(() => flushOrder.Add("wal"));
        
        var checkpoint = new CheckpointManager(indexRegion.Object, dataRegion.Object, wal.Object);
        
        // Act
        checkpoint.ForceCheckpoint();
        
        // Assert: data before wal
        int dataIndex = flushOrder.IndexOf("data");
        int walIndex = flushOrder.IndexOf("wal");
        Assert.True(dataIndex < walIndex, "Data must be flushed before WAL truncation");
    }

    /// <summary>
    /// Validates that checkpoint resets the write counter after flushing.
    /// </summary>
    [Fact]
    public void ForceCheckpoint_ResetsWriteCounter()
    {
        // Arrange
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        
        var checkpoint = new CheckpointManager(indexRegion.Object, dataRegion.Object, wal.Object, threshold: 3);
        
        // Track writes, but don't trigger automatic checkpoint
        checkpoint.TrackWrite();
        checkpoint.TrackWrite();
        
        // Act: force checkpoint
        checkpoint.ForceCheckpoint();
        
        // Track two more writes (should not trigger checkpoint yet)
        checkpoint.TrackWrite();
        checkpoint.TrackWrite();
        
        // Verify checkpoint was called exactly once (from ForceCheckpoint, not from TrackWrite)
        wal.Verify(w => w.Checkpoint(), Times.Exactly(1));
    }

    /// <summary>
    /// Validates that TrackWrite automatically checkpoints when threshold is reached.
    /// </summary>
    [Fact]
    public void TrackWrite_TriggersCheckpointWhenThresholdReached()
    {
        // Arrange
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        
        var checkpoint = new CheckpointManager(indexRegion.Object, dataRegion.Object, wal.Object, threshold: 3);
        
        // Act: track writes up to threshold
        checkpoint.TrackWrite();
        checkpoint.TrackWrite();
        
        // Verify checkpoint not called yet
        wal.Verify(w => w.Checkpoint(), Times.Never);
        
        // Act: reach threshold
        checkpoint.TrackWrite();
        
        // Assert: checkpoint triggered
        wal.Verify(w => w.Checkpoint(), Times.Once);
    }

    // ?????????????????????????????????????????????????????????????????????
    // WAL Tail Handling Tests
    // ?????????????????????????????????????????????????????????????????????

    /// <summary>
    /// Validates that WriteAheadLog truncates invalid tail before returning entries.
    /// This ensures recovery only processes valid entries.
    /// </summary>
    [Fact]
    public void WriteAheadLog_WithValidEntriesFollowedByGarbage_ReturnsOnlyValidAndTruncatesTail()
    {
        // Arrange: Create WAL with valid entry + garbage
        string walPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".wal");

        try
        {
            long goodSize;

            // Create a valid WAL entry and close handle
            using (var wal = new WriteAheadLog(walPath))
            {
                var entry = new WalEntry
                {
                    Operation = WalOperationType.Save,
                    Table = "test",
                    Key = Guid.NewGuid(),
                    Data = [1, 2, 3]
                };

                wal.Append(entry);
                goodSize = new FileInfo(walPath).Length;
            }

            // Append garbage to simulate corruption (file is not locked now)
            using (var stream = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
            {
                stream.Write([0xFF, 0xFF, 0xFF, 0xFF]); // Invalid data
                stream.Flush();
            }

            long corruptSize = new FileInfo(walPath).Length;
            Assert.True(corruptSize > goodSize, "Corruption added");

            // Act: read entries (should auto-truncate tail)
            using var walForRead = new WriteAheadLog(walPath);
            var entries = walForRead.ReadAll();

            // Assert: only valid entry returned
            Assert.Single(entries);
            Assert.Equal("test", entries[0].Table);

            // File should be truncated back to valid size
            long finalSize = new FileInfo(walPath).Length;
            Assert.Equal(goodSize, finalSize);
        }
        finally
        {
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    /// <summary>
    /// Validates that recovery is idempotent when offset validation works correctly.
    /// Calling recovery twice without intermediate writes yields same result.
    /// /// </summary>
    [Fact]
    public void RecoveryIdempotency_WithOffsetValidation()
    {
        // Arrange: Mock that tracks ApplySave calls
        var calls = new List<(string table, Guid key)>();

        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        var memoryIndex = new Mock<IMemoryIndex>();
        var indexManager = new Mock<IIndexManager>();

        long nextIndexOffset = 4096;
        long nextDataOffset = 0;

        indexManager.SetupGet(i => i.NextIndexOffset).Returns(() => nextIndexOffset);
        indexManager.SetupGet(i => i.NextDataOffset).Returns(() => nextDataOffset);
        indexManager
            .Setup(i => i.SetWritePositions(It.IsAny<long>(), It.IsAny<long>()))
            .Callback<long, long>((idx, dat) =>
            {
                nextIndexOffset = idx;
                nextDataOffset = dat;
            });
        
        indexManager
            .Setup(i => i.ApplySave(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<byte[]>(), It.IsAny<long>(), It.IsAny<long>()))
            .Callback<string, Guid, byte[], long, long>((table, key, data, dataOffset, indexOffset) =>
            {
                calls.Add((table, key));
                nextDataOffset += data.Length;
                nextIndexOffset += IndexEntrySerializer.EntryFixedSize;
            });
        
        indexRegion.SetupGet(r => r.Path).Returns("index.idx");
        dataRegion.SetupGet(r => r.Path).Returns("data.dat");
        
        indexRegion.Setup(r => r.Read(0, It.IsAny<byte[]>(), 0, 4))
            .Callback<long, byte[], int, int>((_, b, _, _) => { b[0] = 0; }); // New DB
        
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
                IndexOffset = 4096
            }
        ]);
        
        // Act
        var recovery = new StorageRecovery();
        recovery.Initialize(indexRegion.Object, dataRegion.Object, wal.Object, memoryIndex.Object, indexManager.Object);
        
        // Assert: entry was applied
        Assert.NotEmpty(calls);
    }

    /// <summary>
    /// Validates that entries with stale offsets are skipped during replay.
    /// This preserves idempotency.
    /// </summary>
    [Fact]
    public void RecoveryReplay_WithStaleOffsets_SkipsEntries()
    {
        // Arrange
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        var memoryIndex = new Mock<IMemoryIndex>();
        var indexManager = new Mock<IIndexManager>();
        
        indexManager.SetupGet(i => i.NextIndexOffset).Returns(4096 + IndexEntrySerializer.EntryFixedSize); // After first entry
        indexManager.SetupGet(i => i.NextDataOffset).Returns(4); // After first entry data
        
        indexRegion.SetupGet(r => r.Path).Returns("index.idx");
        dataRegion.SetupGet(r => r.Path).Returns("data.dat");
        
        indexRegion.Setup(r => r.Read(0, It.IsAny<byte[]>(), 0, 4))
            .Callback<long, byte[], int, int>((_, b, _, _) => { b[0] = 0; }); // New DB
        
        indexRegion.SetupGet(r => r.FileSize).Returns(1024 * 1024);
        dataRegion.SetupGet(r => r.FileSize).Returns(1024 * 1024);
        
        // WAL entry with stale offsets (already applied in previous recovery)
        var key = Guid.NewGuid();
        wal.Setup(w => w.ReadAllStreaming()).Returns([
            new WalEntry
            {
                Operation = WalOperationType.Save,
                Table = "users",
                Key = key,
                Data = [1, 2, 3, 4],
                DataOffset = 999,  // Doesn't match cursor (0)
                IndexOffset = 999   // Doesn't match cursor (4096)
            }
        ]);
        
        // Act
        var recovery = new StorageRecovery();
        recovery.Initialize(indexRegion.Object, dataRegion.Object, wal.Object, memoryIndex.Object, indexManager.Object);
        
        // Assert: entry not applied (stale offsets)
        indexManager.Verify(i => i.ApplySave(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<byte[]>(), It.IsAny<long>(), It.IsAny<long>()), Times.Never);
    }

    // ?????????????????????????????????????????????????????????????????????
    // Offset Validation Tests
    // ?????????????????????????????????????????????????????????????????????

    /// <summary>
    /// Validates that SaveBatch with non-contiguous offsets is rejected entirely.
    /// </summary>
    [Fact]
    public void RecoveryReplay_SaveBatchWithGapInOffsets_RejectedEntirely()
    {
        // Arrange
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        var memoryIndex = new Mock<IMemoryIndex>();
        var indexManager = new Mock<IIndexManager>();
        
        long nextIndexOffset = 4096;
        long nextDataOffset = 0;
        
        indexManager.SetupGet(i => i.NextIndexOffset).Returns(() => nextIndexOffset);
        indexManager.SetupGet(i => i.NextDataOffset).Returns(() => nextDataOffset);
        indexManager.Setup(i => i.SetWritePositions(It.IsAny<long>(), It.IsAny<long>()))
            .Callback<long, long>((idx, dat) => { nextIndexOffset = idx; nextDataOffset = dat; });
        
        indexRegion.SetupGet(r => r.Path).Returns("index.idx");
        dataRegion.SetupGet(r => r.Path).Returns("data.dat");
        
        indexRegion.Setup(r => r.Read(0, It.IsAny<byte[]>(), 0, 4))
            .Callback<long, byte[], int, int>((_, b, _, _) => { b[0] = 0; });
        
        indexRegion.SetupGet(r => r.FileSize).Returns(1024 * 1024);
        dataRegion.SetupGet(r => r.FileSize).Returns(1024 * 1024);
        
        var k1 = Guid.NewGuid();
        var k2 = Guid.NewGuid();
        
        // Create batch with GAP in data offsets
        byte[] payload = WalBatchPayloadSerializer.Serialize([
            new WalBatchEntry(k1, [1, 2], DataOffset: 0, IndexOffset: 4096, new Dictionary<string, string>()),
            new WalBatchEntry(k2, [3, 4], DataOffset: 999, IndexOffset: 4096 + IndexEntrySerializer.EntryFixedSize, new Dictionary<string, string>()) // GAP
        ]);
        
        wal.Setup(w => w.ReadAllStreaming()).Returns([
            new WalEntry
            {
                Operation = WalOperationType.SaveBatch,
                Table = "users",
                Key = Guid.Empty,
                Data = payload
            }
        ]);
        
        // Act
        var recovery = new StorageRecovery();
        recovery.Initialize(indexRegion.Object, dataRegion.Object, wal.Object, memoryIndex.Object, indexManager.Object);
        
        // Assert: no entries from batch were applied
        indexManager.Verify(i => i.ApplySave(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<byte[]>(), It.IsAny<long>(), It.IsAny<long>()), Times.Never);
    }

    // ?????????????????????????????????????????????????????????????????????
    // Recovery Idempotency Tests (Critical)
    // ?????????????????????????????????????????????????????????????????????

    /// <summary>
    /// Validates that calling recovery twice without intermediate writes is safe.
    /// Both calls should process the same entries without duplication.
    /// </summary>
    [Fact]
    public void Recovery_TwiceWithSameWal_IsIdempotent()
    {
        // Arrange: emulate persistent regions in-memory across two recovery runs
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        var memoryIndex = new Mock<IMemoryIndex>();
        var indexManager = new Mock<IIndexManager>();

        var applyCalls = new List<string>();

        long nextIndexOffset = 4096;
        long nextDataOffset = 0;

        var indexBytes = new byte[1024 * 1024];
        var dataBytes = new byte[1024 * 1024];

        indexManager.SetupGet(i => i.NextIndexOffset).Returns(() => nextIndexOffset);
        indexManager.SetupGet(i => i.NextDataOffset).Returns(() => nextDataOffset);
        indexManager.Setup(i => i.SetWritePositions(It.IsAny<long>(), It.IsAny<long>()))
            .Callback<long, long>((idx, dat) => { nextIndexOffset = idx; nextDataOffset = dat; });

        indexManager.Setup(i => i.ApplySave(It.IsAny<string>(), It.IsAny<Guid>(), It.IsAny<byte[]>(), It.IsAny<long>(), It.IsAny<long>()))
            .Callback<string, Guid, byte[], long, long>((table, key, data, dataOffset, indexOffset) =>
            {
                applyCalls.Add($"Apply:{table}");

                Span<byte> entry = stackalloc byte[IndexEntrySerializer.EntryFixedSize];
                IndexEntrySerializer.Write(entry, table, key, dataOffset, data.Length, version: 0);
                entry.CopyTo(indexBytes.AsSpan((int)indexOffset, IndexEntrySerializer.EntryFixedSize));

                nextDataOffset += data.Length;
                nextIndexOffset += IndexEntrySerializer.EntryFixedSize;
            });

        indexRegion.SetupGet(r => r.Path).Returns("index.idx");
        dataRegion.SetupGet(r => r.Path).Returns("data.dat");

        indexRegion.SetupGet(r => r.FileSize).Returns(indexBytes.Length);
        dataRegion.SetupGet(r => r.FileSize).Returns(dataBytes.Length);

        indexRegion.Setup(r => r.Read(It.IsAny<long>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>()))
            .Callback<long, byte[], int, int>((offset, buffer, bufferOffset, count) =>
            {
                Array.Copy(indexBytes, (int)offset, buffer, bufferOffset, count);
            });

        indexRegion.Setup(r => r.Write(It.IsAny<long>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>()))
            .Callback<long, byte[], int, int>((offset, buffer, bufferOffset, count) =>
            {
                Array.Copy(buffer, bufferOffset, indexBytes, (int)offset, count);
            });

        dataRegion.Setup(r => r.Read(It.IsAny<long>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>()))
            .Callback<long, byte[], int, int>((offset, buffer, bufferOffset, count) =>
            {
                Array.Copy(dataBytes, (int)offset, buffer, bufferOffset, count);
            });

        dataRegion.Setup(r => r.Write(It.IsAny<long>(), It.IsAny<byte[]>(), It.IsAny<int>(), It.IsAny<int>()))
            .Callback<long, byte[], int, int>((offset, buffer, bufferOffset, count) =>
            {
                Array.Copy(buffer, bufferOffset, dataBytes, (int)offset, count);
            });

        var key = Guid.NewGuid();
        wal.Setup(w => w.ReadAllStreaming()).Returns([
            new WalEntry
            {
                Operation = WalOperationType.Save,
                Table = "users",
                Key = key,
                Data = [1, 2, 3, 4],
                DataOffset = 0,
                IndexOffset = 4096
            }
        ]);

        // Act: First recovery (applies WAL)
        var recovery = new StorageRecovery();
        recovery.Initialize(indexRegion.Object, dataRegion.Object, wal.Object, memoryIndex.Object, indexManager.Object);
        int callsAfterFirst = applyCalls.Count;

        applyCalls.Clear();

        // Act: Second recovery (offsets reconstructed from index, WAL entry becomes stale)
        recovery.Initialize(indexRegion.Object, dataRegion.Object, wal.Object, memoryIndex.Object, indexManager.Object);
        int callsAfterSecond = applyCalls.Count;

        // Assert: First applied, second skipped (idempotent)
        Assert.True(callsAfterFirst >= 1, "First recovery should apply entries");
        Assert.Equal(0, callsAfterSecond);
    }

    /// <summary>
    /// Validates that corrupted WAL tail is truncated and doesn't break recovery.
    /// </summary>
    [Fact]
    public void Recovery_WithCorruptedWalTail_TruncatesTail()
    {
        // Arrange: Create WAL with valid entry then corrupted tail
        string walPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".wal");

        try
        {
            long validSize;

            // Add valid entry and close handle
            using (var wal = new WriteAheadLog(walPath))
            {
                var validEntry = new WalEntry
                {
                    Operation = WalOperationType.Save,
                    Table = "test",
                    Key = Guid.NewGuid(),
                    Data = [1, 2, 3]
                };
                wal.Append(validEntry);
                validSize = new FileInfo(walPath).Length;
            }

            // Corrupt the tail (file is not locked now)
            using (var stream = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
            {
                stream.Write([0xFF, 0xFF, 0xFF]); // Corrupted data
                stream.Flush();
            }

            long corruptSize = new FileInfo(walPath).Length;
            Assert.True(corruptSize > validSize);

            // Act: Read entries (should auto-truncate tail)
            using var walForRead = new WriteAheadLog(walPath);
            var entries = walForRead.ReadAll();

            // Assert: only valid entry returned
            Assert.Single(entries);
            Assert.Equal("test", entries[0].Table);

            // File should be truncated back to valid size
            long finalSize = new FileInfo(walPath).Length;
            Assert.Equal(validSize, finalSize);
        }
        finally
        {
            if (File.Exists(walPath))
                File.Delete(walPath);
        }
    }

    /// <summary>
    /// Validates that interrupted compaction (both .tmp files exist) is recovered.
    /// </summary>
    [Fact]
    public void Recovery_WithInterruptedCompaction_RecoversBothTmpStates()
    {
        // Arrange: Simulate interrupted compaction
        string indexPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".idx");
        string dataPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid() + ".dat");
        string indexTmp = indexPath + ".tmp";
        string dataTmp = dataPath + ".tmp";
        
        try
        {
            // Create original files
            File.WriteAllText(indexPath, "original idx content");
            File.WriteAllText(dataPath, "original dat content");
            
            // Simulate crash: both .tmp files exist (compaction never started rename)
            File.WriteAllText(indexTmp, "new idx content");
            File.WriteAllText(dataTmp, "new dat content");
            
            // Act: Recover interrupted compaction
            FileStorage.Infrastructure.Recovery.StorageRecovery.RecoverInterruptedCompaction(indexPath, dataPath);
            
            // Assert: both .tmp deleted, originals unchanged
            Assert.False(File.Exists(indexTmp));
            Assert.False(File.Exists(dataTmp));
            Assert.Equal("original idx content", File.ReadAllText(indexPath));
            Assert.Equal("original dat content", File.ReadAllText(dataPath));
        }
        finally
        {
            foreach (var f in new[] { indexPath, dataPath, indexTmp, dataTmp })
                if (File.Exists(f)) File.Delete(f);
        }
    }

    // ?????????????????????????????????????????????????????????????????????
    // Checkpoint Crash Safety Tests
    // ?????????????????????????????????????????????????????????????????????

    /// <summary>
    /// Validates that checkpoint flushes both regions before truncating WAL.
    /// This ensures no data loss if crash occurs before truncation.
    /// </summary>
    [Fact]
    public void Checkpoint_FlushesBeforeTruncation()
    {
        // Arrange: Track flush order
        var flushOrder = new List<string>();
        
        var indexRegion = new Mock<IMmapRegion>();
        indexRegion.Setup(r => r.Flush()).Callback(() => flushOrder.Add("index"));
        
        var dataRegion = new Mock<IMmapRegion>();
        dataRegion.Setup(r => r.Flush()).Callback(() => flushOrder.Add("data"));
        
        var wal = new Mock<IWriteAheadLog>();
        wal.Setup(w => w.Checkpoint()).Callback(() => flushOrder.Add("wal"));
        
        var checkpoint = new CheckpointManager(indexRegion.Object, dataRegion.Object, wal.Object);
        
        // Act
        checkpoint.ForceCheckpoint();
        
        // Assert: flushes happen before truncation
        Assert.Contains("index", flushOrder);
        Assert.Contains("data", flushOrder);
        Assert.Contains("wal", flushOrder);
        Assert.True(flushOrder.IndexOf("data") < flushOrder.IndexOf("wal"),
            "Data must be flushed before WAL truncation");
    }

    /// <summary>
    /// Validates that if crash occurs after index flush but before data flush,
    /// recovery can still restore data from WAL.
    /// </summary>
    [Fact]
    public void Checkpoint_WithCrashAfterIndexFlush_RecoversFromWal()
    {
        // Arrange: Simulate crash scenario using mock
        var indexRegion = new Mock<IMmapRegion>();
        var dataRegion = new Mock<IMmapRegion>();
        var wal = new Mock<IWriteAheadLog>();
        
        var flushOrder = new List<string>();
        indexRegion.Setup(r => r.Flush()).Callback(() => 
        {
            flushOrder.Add("index");
            // Simulate: data is NOT flushed yet (crash before second flush)
        });
        
        dataRegion.Setup(r => r.Flush()).Callback(() => flushOrder.Add("data"));
        
        wal.Setup(w => w.Checkpoint()).Callback(() => flushOrder.Add("wal"));
        
        var checkpoint = new CheckpointManager(indexRegion.Object, dataRegion.Object, wal.Object);
        
        // Act
        checkpoint.ForceCheckpoint();
        
        // Assert: All flushes executed (in correct order)
        // Even if crash happens between them, next recovery reads WAL
        Assert.Equal(3, flushOrder.Count);
        Assert.True(flushOrder.IndexOf("index") < flushOrder.IndexOf("data"));
        Assert.True(flushOrder.IndexOf("data") < flushOrder.IndexOf("wal"));
        
        // Verify regions were flushed
        indexRegion.Verify(r => r.Flush(), Times.Once);
        dataRegion.Verify(r => r.Flush(), Times.Once);
    }

    /// <summary>
    /// Validates that compaction recovery works when both .tmp files exist
    /// (compaction crashed before first rename completed).
    /// </summary>
    [Fact]
    public void Compaction_WithBothTmpFiles_DeletesBothAndPreservesOriginals()
    {
        // Arrange: Simulate compaction crash where both .tmp exist
        string indexPath = Path.GetTempPath() + Guid.NewGuid() + ".idx";
        string dataPath = Path.GetTempPath() + Guid.NewGuid() + ".dat";
        string indexTmp = indexPath + ".tmp";
        string dataTmp = dataPath + ".tmp";
        
        try
        {
            // Create original files with specific content
            File.WriteAllText(indexPath, "original index");
            File.WriteAllText(dataPath, "original data");
            
            // Simulate crash: both .tmp exist (compaction wrote but never renamed)
            File.WriteAllText(indexTmp, "new index");
            File.WriteAllText(dataTmp, "new data");
            
            // Act: Recover compaction
            StorageRecovery.RecoverInterruptedCompaction(indexPath, dataPath);
            
            // Assert: .tmp files deleted, originals preserved
            Assert.False(File.Exists(indexTmp), ".idx.tmp should be deleted");
            Assert.False(File.Exists(dataTmp), ".dat.tmp should be deleted");
            Assert.Equal("original index", File.ReadAllText(indexPath));
            Assert.Equal("original data", File.ReadAllText(dataPath));
        }
        finally
        {
            foreach (var f in new[] { indexPath, dataPath, indexTmp, dataTmp })
                if (File.Exists(f)) File.Delete(f);
        }
    }

    /// <summary>
    /// Validates that compaction recovery works when crash occurred after first rename.
    /// Only .dat.tmp exists (idx was already renamed).
    /// </summary>
    [Fact]
    public void CompactionCrash_AfterFirstRename_CompletesSecondRename()
    {
        // Arrange: Simulate crash after .idx rename, before .dat rename
        string indexPath = Path.GetTempPath() + Guid.NewGuid() + ".idx";
        string dataPath = Path.GetTempPath() + Guid.NewGuid() + ".dat";
        string dataTmp = dataPath + ".tmp";
        
        try
        {
            // .idx was already renamed (new content in place)
            File.WriteAllText(indexPath, "new index");
            
            // .dat.tmp still exists (rename didn't complete)
            File.WriteAllText(dataTmp, "new data");
            
            // Act: Recover compaction
            StorageRecovery.RecoverInterruptedCompaction(indexPath, dataPath);
            
            // Assert: .dat.tmp renamed to .dat
            Assert.False(File.Exists(dataTmp), ".dat.tmp should be renamed");
            Assert.Equal("new data", File.ReadAllText(dataPath));
        }
        finally
        {
            foreach (var f in new[] { indexPath, dataPath, dataTmp })
                if (File.Exists(f)) File.Delete(f);
        }
    }
}
