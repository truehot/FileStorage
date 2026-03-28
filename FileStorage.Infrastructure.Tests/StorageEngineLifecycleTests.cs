using FileStorage.Abstractions;
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
using Moq;

namespace FileStorage.Infrastructure.Tests;

public class StorageEngineLifecycleTests
{
    [Fact]
    public async Task Dispose_WaitsWhileReadIsInProgress()
    {
        var context = new EngineTestContext();
        var key = Guid.NewGuid();
        long indexOffset = 128;
        var readEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRead = new ManualResetEventSlim(false);

        context.MemoryIndex
            .Setup(m => m.TryGet("users", key, out indexOffset))
            .Returns(true);

        context.RecordReader
            .Setup(r => r.Read(It.IsAny<IMmapRegion>(), It.IsAny<IMmapRegion>(), It.IsAny<byte[]>(), indexOffset, "users", key))
            .Callback(() =>
            {
                readEntered.TrySetResult();
                releaseRead.Wait();
            })
            .Returns(new StorageRecord("users", key, [1, 2, 3], 0, false));

        using var engine = context.CreateEngine();
        await engine.InitializeAsync();

        var readTask = Task.Run(async () => await engine.GetByKeyAsync("users", key));
        await readEntered.Task;

        var disposeTask = Task.Run(() => engine.Dispose());
        await Task.Delay(100);
        Assert.False(disposeTask.IsCompleted);

        releaseRead.Set();

        await readTask;
        await disposeTask;
    }

    [Fact]
    public async Task Dispose_WaitsWhileWriteIsInProgress()
    {
        var context = new EngineTestContext();
        var writeEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseWrite = new ManualResetEventSlim(false);

        context.IndexManager.SetupGet(m => m.NextDataOffset).Returns(0L);
        context.IndexManager.SetupGet(m => m.NextIndexOffset).Returns(0L);
        context.Wal
            .Setup(w => w.Append(It.IsAny<WalEntry>()))
            .Callback(() =>
            {
                writeEntered.TrySetResult();
                releaseWrite.Wait();
            })
            .Returns(1L);

        using var engine = context.CreateEngine();
        await engine.InitializeAsync();

        var writeTask = Task.Run(async () => await engine.SaveAsync("users", Guid.NewGuid(), [1, 2, 3]));
        await writeEntered.Task;

        var disposeTask = Task.Run(() => engine.Dispose());
        await Task.Delay(100);
        Assert.False(disposeTask.IsCompleted);

        releaseWrite.Set();

        await writeTask;
        await disposeTask;
    }

    [Fact]
    public async Task Dispose_WaitsWhileStreamIsInProgress()
    {
        var context = new EngineTestContext();
        var key = Guid.NewGuid();
        var streamEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseStream = new ManualResetEventSlim(false);

        context.MemoryIndex
            .Setup(m => m.GetByTable("users", 0, int.MaxValue))
            .Returns([(key, 256L)]);

        context.IndexManager.SetupGet(m => m.EntrySize).Returns(64);
        context.RecordReader
            .Setup(r => r.Read(It.IsAny<IMmapRegion>(), It.IsAny<IMmapRegion>(), It.IsAny<byte[]>(), 256L, "users", key))
            .Callback(() =>
            {
                streamEntered.TrySetResult();
                releaseStream.Wait();
            })
            .Returns(new StorageRecord("users", key, [5, 6, 7], 0, false));

        using var engine = context.CreateEngine();
        await engine.InitializeAsync();

        var streamTask = Task.Run(async () =>
        {
            await foreach (var record in engine.GetByTableStreamAsync("users"))
            {
                return record;
            }

            return null;
        });

        await streamEntered.Task;

        var disposeTask = Task.Run(() => engine.Dispose());
        await Task.Delay(100);
        Assert.False(disposeTask.IsCompleted);

        releaseStream.Set();

        await streamTask;
        await disposeTask;
    }

    [Fact]
    public async Task SyncDispose_WaitsForReaders()
    {
        var context = new EngineTestContext();
        var readEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRead = new ManualResetEventSlim(false);

        context.MemoryIndex
            .Setup(m => m.CountByTable("users"))
            .Callback(() =>
            {
                readEntered.TrySetResult();
                releaseRead.Wait();
            })
            .Returns(1L);

        using var engine = context.CreateEngine();
        await engine.InitializeAsync();

        var countTask = Task.Run(async () => await engine.CountAsync("users"));
        await readEntered.Task;

        var disposeTask = Task.Run(() => engine.Dispose());
        await Task.Delay(100);
        Assert.False(disposeTask.IsCompleted);

        releaseRead.Set();

        await countTask;
        await disposeTask;
    }

    [Fact]
    public async Task NewOperations_ThrowAfterDisposeBegins()
    {
        var context = new EngineTestContext();
        var readEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseRead = new ManualResetEventSlim(false);

        context.MemoryIndex
            .Setup(m => m.CountByTable("users"))
            .Callback(() =>
            {
                readEntered.TrySetResult();
                releaseRead.Wait();
            })
            .Returns(1L);

        using var engine = context.CreateEngine();
        await engine.InitializeAsync();

        var blockingReadTask = Task.Run(async () => await engine.CountAsync("users"));
        await readEntered.Task;

        var disposeTask = Task.Run(() => engine.Dispose());

        var threw = await EventuallyThrowsDisposedAsync(() => engine.ListTablesAsync());
        Assert.True(threw);

        releaseRead.Set();

        await blockingReadTask;
        await disposeTask;
    }

    private static async Task<bool> EventuallyThrowsDisposedAsync(Func<Task> operation, int attempts = 50, int delayMs = 20)
    {
        for (int i = 0; i < attempts; i++)
        {
            try
            {
                await operation();
            }
            catch (ObjectDisposedException)
            {
                return true;
            }

            await Task.Delay(delayMs);
        }

        return false;
    }

    private sealed class EngineTestContext
    {
        public Mock<IMmapRegion> IndexRegion { get; } = new();
        public Mock<IMmapRegion> DataRegion { get; } = new();
        public Mock<IWriteAheadLog> Wal { get; } = new();
        public Mock<ICheckpointManager> Checkpoint { get; } = new();
        public Mock<IStorageRecovery> Recovery { get; } = new();
        public Mock<IIndexManager> IndexManager { get; } = new();
        public Mock<ISecondaryIndexManager> SecondaryIndex { get; } = new();
        public Mock<IRecordReader> RecordReader { get; } = new();
        public Mock<IMemoryIndex> MemoryIndex { get; } = new();
        public IRegionProvider Regions { get; }
        public ICompactionService Compaction { get; } = new CompactionService();

        public EngineTestContext()
        {
            Regions = new RegionProvider(IndexRegion.Object, DataRegion.Object);

            Wal.Setup(w => w.ReadAllStreaming()).Returns([]);
            Recovery.Setup(r => r.Initialize(
                    It.IsAny<IMmapRegion>(),
                    It.IsAny<IMmapRegion>(),
                    It.IsAny<IWriteAheadLog>(),
                    It.IsAny<IMemoryIndex>(),
                    It.IsAny<IIndexManager>()))
                .Returns(new StorageRecovery.RecoveryResult(4096, 0));

            IndexManager.SetupGet(m => m.EntrySize).Returns(64);
            MemoryIndex.Setup(m => m.GetTableNames()).Returns([]);
            MemoryIndex.Setup(m => m.GetByTable(It.IsAny<string>(), It.IsAny<int>(), It.IsAny<int>()))
                .Returns([]);
            MemoryIndex.Setup(m => m.TableExists(It.IsAny<string>())).Returns(false);
            MemoryIndex.Setup(m => m.CountByTable(It.IsAny<string>())).Returns(0L);
        }

        public StorageEngine CreateEngine()
        {
            var checkpointHandle = new CheckpointHandle(Checkpoint.Object);
            var lifetime = new StorageEngineLifetime();
            var replayService = new SecondaryIndexReplayService(Wal.Object, SecondaryIndex.Object);
            var startupOperations = new StorageStartupOperations(
                Regions,
                Wal.Object,
                MemoryIndex.Object,
                IndexManager.Object,
                Recovery.Object,
                SecondaryIndex.Object,
                replayService);
            var readOperations = new StorageReadOperations(Regions, MemoryIndex.Object, IndexManager.Object, RecordReader.Object);
            var writeOperations = new StorageWriteOperations(Wal.Object, MemoryIndex.Object, IndexManager.Object, SecondaryIndex.Object, checkpointHandle);
            var indexOperations = new StorageIndexOperations(SecondaryIndex.Object);
            var maintenanceOperations = new StorageMaintenanceOperations(
                Regions,
                MemoryIndex.Object,
                IndexManager.Object,
                Wal.Object,
                Compaction,
                checkpointHandle);

            return new StorageEngine(
                Regions,
                Wal.Object,
                SecondaryIndex.Object,
                checkpointHandle,
                lifetime,
                startupOperations,
                readOperations,
                writeOperations,
                indexOperations,
                maintenanceOperations);
        }
    }
}
