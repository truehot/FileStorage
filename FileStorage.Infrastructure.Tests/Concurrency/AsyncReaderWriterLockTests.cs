using FileStorage.Infrastructure.Core.Concurrency;

namespace FileStorage.Infrastructure.Tests.Concurrency;

public class AsyncReaderWriterLockTests
{
    [Fact]
    public async Task AcquireWriteLock_WaitsForExistingReader()
    {
        using var rwLock = new AsyncReaderWriterLock();
        using var readLock = await rwLock.AcquireReadLockAsync();

        var writerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writerAcquired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var writerTask = Task.Run(() =>
        {
            writerStarted.SetResult();
            using var _ = rwLock.AcquireWriteLock();
            writerAcquired.SetResult();
        });

        await writerStarted.Task;
        await Task.Delay(100);
        Assert.False(writerAcquired.Task.IsCompleted);

        readLock.Dispose();

        await writerTask;
        Assert.True(writerAcquired.Task.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task Dispose_WakesWaitingSynchronousWriter()
    {
        using var rwLock = new AsyncReaderWriterLock();
        using var readLock = await rwLock.AcquireReadLockAsync();

        var writerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var writerTask = Task.Run(() =>
        {
            writerStarted.SetResult();
            Assert.Throws<ObjectDisposedException>(() =>
            {
                using var _ = rwLock.AcquireWriteLock();
            });
        });

        await writerStarted.Task;
        await Task.Delay(100);

        rwLock.Dispose();
        readLock.Dispose();

        await writerTask;
    }
}
