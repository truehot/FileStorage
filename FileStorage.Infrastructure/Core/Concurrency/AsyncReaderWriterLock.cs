namespace FileStorage.Infrastructure.Core.Concurrency;

/// <summary>
/// Async-compatible reader-writer lock with writer-priority fairness
/// and optional lease-based expiration for hung lock holders.
/// <para>
/// • Multiple concurrent readers OR a single exclusive writer.<br/>
/// • Writer-priority: when a writer is queued, new readers wait behind it.<br/>
/// • Lock-free reader entry when no writer is active or waiting.<br/>
/// • Optional <see cref="LeaseTimeout"/>: if a holder doesn't release within the timeout,
///   the lock auto-releases and logs a warning — prevents permanent deadlocks from hung readers.
/// </para>
/// </summary>
internal sealed class AsyncReaderWriterLock : IDisposable
{
    private readonly object _sync = new();
    private int _readerCount;
    private bool _writerActive;
    private int _syncWritersWaiting;

    private readonly Queue<TaskCompletionSource<IDisposable>> _writerQueue = new();
    private readonly Queue<TaskCompletionSource<IDisposable>> _readerQueue = new();

    private bool _disposed;

    /// <summary>
    /// Maximum time a lock holder may keep the lock before it is forcibly released.
    /// <c>null</c> = no timeout (default). Set to e.g. <c>TimeSpan.FromSeconds(30)</c>
    /// to protect against hung readers blocking writers forever.
    /// </summary>
    public TimeSpan? LeaseTimeout { get; init; }

    /// <summary>
    /// Raised when a lease expires and the lock is forcibly released.
    /// Useful for logging / diagnostics.
    /// </summary>
    public event Action<string>? LeaseExpired;

    /// <summary>
    /// Acquires a read lock asynchronously.
    /// </summary>
    public Task<IDisposable> AcquireReadLockAsync(CancellationToken ct = default)
    {
        lock (_sync)
        {
            ThrowIfDisposed();

            if (!_writerActive && _writerQueue.Count == 0 && _syncWritersWaiting == 0)
            {
                _readerCount++;
                return Task.FromResult<IDisposable>(CreateReadReleaser());
            }

            var tcs = new TaskCompletionSource<IDisposable>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (ct.CanBeCanceled)
                ct.Register(() => CancelWaiter(tcs), useSynchronizationContext: false);

            _readerQueue.Enqueue(tcs);
            return tcs.Task;
        }
    }

    /// <summary>
    /// Acquires a write lock asynchronously.
    /// </summary>
    public Task<IDisposable> AcquireWriteLockAsync(CancellationToken ct = default)
    {
        lock (_sync)
        {
            ThrowIfDisposed();

            if (!_writerActive && _readerCount == 0)
            {
                _writerActive = true;
                return Task.FromResult<IDisposable>(CreateWriteReleaser());
            }

            var tcs = new TaskCompletionSource<IDisposable>(TaskCreationOptions.RunContinuationsAsynchronously);

            if (ct.CanBeCanceled)
                ct.Register(() => CancelWaiter(tcs), useSynchronizationContext: false);

            _writerQueue.Enqueue(tcs);
            return tcs.Task;
        }
    }

    /// <summary>
    /// Acquires a write lock synchronously.
    /// </summary>
    public IDisposable AcquireWriteLock(CancellationToken ct = default)
    {
        lock (_sync)
        {
            ThrowIfDisposed();
            _syncWritersWaiting++;
            try
            {
                while (_writerActive || _readerCount > 0 || _writerQueue.Count > 0)
                {
                    ct.ThrowIfCancellationRequested();
                    Monitor.Wait(_sync, TimeSpan.FromMilliseconds(50));
                    ThrowIfDisposed();
                }

                _writerActive = true;
                return CreateWriteReleaser();
            }
            finally
            {
                _syncWritersWaiting--;
            }
        }
    }

    private void ReleaseReadLock()
    {
        lock (_sync)
        {
            if (--_readerCount > 0)
                return;

            if (_writerQueue.Count > 0)
            {
                WakeNextWriter();
            }
            else if (_syncWritersWaiting > 0)
            {
                _writerActive = false;
                Monitor.PulseAll(_sync);
            }
            else
            {
                WakeAllReaders();
            }
        }
    }

    private void ReleaseWriteLock()
    {
        lock (_sync)
        {
            if (_writerQueue.Count > 0)
            {
                WakeNextWriter();
                return;
            }

            _writerActive = false;

            if (_syncWritersWaiting > 0)
            {
                Monitor.PulseAll(_sync);
                return;
            }

            WakeAllReaders();
        }
    }

    /// <summary>
    /// Dequeues and completes the next non-cancelled writer TCS.
    /// Falls through to <see cref="WakeAllReaders"/> if no writers remain.
    /// Must be called under <see cref="_sync"/>.
    /// </summary>
    private void WakeNextWriter()
    {
        while (_writerQueue.Count > 0)
        {
            var tcs = _writerQueue.Dequeue();
            if (tcs.Task.IsCanceled) continue;

            _writerActive = true;
            tcs.TrySetResult(CreateWriteReleaser());
            return;
        }

        _writerActive = false;

        if (_syncWritersWaiting > 0)
        {
            Monitor.PulseAll(_sync);
            return;
        }

        WakeAllReaders();
    }

    /// <summary>
    /// Dequeues and completes ALL queued reader TCS entries at once.
    /// Must be called under <see cref="_sync"/>.
    /// </summary>
    private void WakeAllReaders()
    {
        while (_readerQueue.Count > 0)
        {
            var tcs = _readerQueue.Dequeue();
            if (tcs.Task.IsCanceled) continue;

            _readerCount++;
            tcs.TrySetResult(CreateReadReleaser());
        }

        Monitor.PulseAll(_sync);
    }

    private void CancelWaiter(TaskCompletionSource<IDisposable> tcs)
    {
        lock (_sync)
        {
            if (!tcs.TrySetCanceled())
                return;

            if (_readerCount == 0 && _writerQueue.All(w => w.Task.IsCanceled))
            {
                if (_syncWritersWaiting > 0)
                    Monitor.PulseAll(_sync);
                else
                    WakeNextWriter();
            }
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    private LockReleaser CreateReadReleaser()
    {
        var releaser = new LockReleaser(this, isWriter: false);
        ScheduleLeaseExpiration(releaser);
        return releaser;
    }

    private LockReleaser CreateWriteReleaser()
    {
        var releaser = new LockReleaser(this, isWriter: true);
        ScheduleLeaseExpiration(releaser);
        return releaser;
    }

    private void ScheduleLeaseExpiration(LockReleaser releaser)
    {
        if (LeaseTimeout is not { } timeout) return;

        _ = Task.Delay(timeout).ContinueWith(_ =>
        {
            if (releaser.TryForceRelease())
            {
                LeaseExpired?.Invoke(
                    releaser.IsWriter
                        ? $"Write lock lease expired after {timeout}. Forcibly released."
                        : $"Read lock lease expired after {timeout}. Forcibly released.");
            }
        }, TaskScheduler.Default);
    }

    private sealed class LockReleaser(AsyncReaderWriterLock owner, bool isWriter) : IDisposable
    {
        private readonly AsyncReaderWriterLock _owner = owner;
        private int _released;

        public bool IsWriter { get; } = isWriter;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _released, 1) == 0)
                Release();
        }

        /// <summary>
        /// Called by the lease timer. Returns true if this call actually released the lock.
        /// </summary>
        public bool TryForceRelease()
        {
            if (Interlocked.Exchange(ref _released, 1) != 0)
                return false;

            Release();
            return true;
        }

        private void Release()
        {
            if (IsWriter)
                _owner.ReleaseWriteLock();
            else
                _owner.ReleaseReadLock();
        }
    }

    public void Dispose()
    {
        lock (_sync)
        {
            if (_disposed)
                return;

            _disposed = true;

            while (_writerQueue.Count > 0)
            {
                var waiter = _writerQueue.Dequeue();
                waiter.TrySetException(new ObjectDisposedException(nameof(AsyncReaderWriterLock)));
            }

            while (_readerQueue.Count > 0)
            {
                var waiter = _readerQueue.Dequeue();
                waiter.TrySetException(new ObjectDisposedException(nameof(AsyncReaderWriterLock)));
            }

            Monitor.PulseAll(_sync);
        }
    }
}