using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;

namespace FileStorage.Infrastructure.Core.IO;

/// <summary>
/// Manages a single memory-mapped file with automatic growth.
/// Uses a snapshot-based accessor swap so that concurrent readers
/// never observe a disposed accessor during file growth.
/// </summary>
/// <remarks>
/// <b>Thread-safety contract:</b>
/// <list type="bullet">
///   <item><see cref="Read"/> may be called concurrently from multiple threads.</item>
///   <item><see cref="Write"/>, <see cref="EnsureCapacity"/> and <see cref="Flush"/> must be called
///         under an exclusive (write) lock at the <c>StorageEngine</c> level.</item>
///   <item><see cref="EnsureCapacity"/> IS safe to call while readers are active —
///         they will complete on the previous snapshot.</item>
/// </list>
/// <para>
/// <b>File handle strategy:</b> A single <see cref="FileStream"/> is kept open for the lifetime
/// of the region via a ref-counted <see cref="SharedFileHandle"/>. All <see cref="MemoryMappedFile"/>
/// instances are created from this shared stream, avoiding "file in use" errors on Windows.
/// The <see cref="FileStream"/> is closed only when the last snapshot is fully released —
/// preventing crashes if late readers still hold an old accessor.
/// </para>
/// </remarks>
internal sealed class MmapRegion : IMmapRegion
{
    private readonly string _path;
    private readonly long _initialSize;
    private readonly long _maxSize;
    private readonly Lock _growLock = new();

    /// <summary>
    /// Ref-counted wrapper around the shared <see cref="FileStream"/>.
    /// Each <see cref="MmapSnapshot"/> holds a reference. The stream is closed
    /// only when the last snapshot releases its reference.
    /// </summary>
    private sealed class SharedFileHandle(FileStream stream)
    {
        public readonly FileStream Stream = stream;
        private int _refCount = 1; // owner (MmapRegion) starts with 1

        /// <summary>
        /// Adds a reference for a new snapshot. Must be called before creating the snapshot.
        /// </summary>
        public void AddRef()
        {
            int current = Volatile.Read(ref _refCount);
            ObjectDisposedException.ThrowIf(current <= 0, this);

            Interlocked.Increment(ref _refCount);
        }

        /// <summary>
        /// Releases a reference. When the last reference is released,
        /// the underlying <see cref="FileStream"/> is closed.
        /// </summary>
        public void Release()
        {
            if (Interlocked.Decrement(ref _refCount) == 0)
            {
                Stream.Dispose();
            }
        }
    }

    /// <summary>
    /// Immutable snapshot of the mmap + accessor pair.
    /// Readers grab a reference, use it, and never see a half-swapped state.
    /// When the last reader releases, the snapshot disposes its mmap + accessor
    /// and releases its reference to the shared file handle.
    /// </summary>
    private sealed class MmapSnapshot(
        MemoryMappedFile mmap,
        MemoryMappedViewAccessor accessor,
        long size,
        SharedFileHandle fileHandle)
    {
        public readonly MemoryMappedFile Mmap = mmap;
        public readonly MemoryMappedViewAccessor Accessor = accessor;
        public readonly long Size = size;
        private readonly SharedFileHandle _fileHandle = fileHandle;
        private int _refCount = 1; // starts with 1 (owner reference)

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryAddRef()
        {
            while (true)
            {
                int current = Volatile.Read(ref _refCount);
                if (current <= 0) return false;
                if (Interlocked.CompareExchange(ref _refCount, current + 1, current) == current)
                    return true;
            }
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _refCount) == 0)
            {
                Accessor.Dispose();
                Mmap.Dispose();
                // Release our hold on the FileStream.
                // If this was the last snapshot, the stream closes now.
                _fileHandle.Release();
            }
        }
    }

    private readonly SharedFileHandle _fileHandle;
    private volatile MmapSnapshot _snapshot;
    private int _disposed; // 0 = active, 1 = disposed; accessed via Interlocked

    public string Path => _path;
    public long FileSize => _snapshot.Size;
    public long InitialSize => _initialSize;
    public long MaxSize => _maxSize;

    public MmapRegion(string path, long initialSize, long maxSize)
    {
        _path = path;
        _initialSize = initialSize;
        _maxSize = maxSize;

        var stream = new FileStream(
            path,
            FileMode.OpenOrCreate,
            FileAccess.ReadWrite,
            FileShare.ReadWrite);

        _fileHandle = new SharedFileHandle(stream);

        long size = stream.Length > initialSize ? stream.Length : initialSize;
        _snapshot = CreateSnapshot(size);
    }

    /// <summary>
    /// Grows the file to fit at least <paramref name="writePos"/> + <paramref name="required"/> bytes.
    /// Must be called under an exclusive write lock. Safe for concurrent readers —
    /// they continue using the previous snapshot until they finish.
    /// </summary>
    public void EnsureCapacity(long writePos, long required)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        var current = _snapshot;
        if (writePos + required <= current.Size) return;

        if (writePos + required > _maxSize)
            throw new InvalidOperationException(
                $"File '{System.IO.Path.GetFileName(_path)}' would exceed maximum size of {_maxSize / (1024 * 1024)} MB.");

        long newSize = current.Size;
        while (newSize < writePos + required)
            newSize = Math.Min(newSize * 2, _maxSize);

        lock (_growLock)
        {
            current = _snapshot;
            if (writePos + required <= current.Size) return;

            // Flush old data to disk before swapping.
            current.Accessor.Flush();

            var next = CreateSnapshot(newSize);
            var old = Interlocked.Exchange(ref _snapshot, next);

            // Release owner's reference. If readers still hold refs,
            // the old accessor + mmap stay alive. FileStream stays alive
            // because the new snapshot also holds a ref to SharedFileHandle.
            old.Release();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Read(long offset, byte[] buffer, int bufferOffset, int count)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        var snap = AcquireSnapshot();
        try
        {
            snap.Accessor.ReadArray(offset, buffer, bufferOffset, count);
        }
        finally
        {
            snap.Release();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Write(long offset, byte[] buffer, int bufferOffset, int count)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        var snap = AcquireSnapshot();
        try
        {
            snap.Accessor.WriteArray(offset, buffer, bufferOffset, count);
        }
        finally
        {
            snap.Release();
        }
    }

    public void Flush()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        _snapshot.Accessor.Flush();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MmapSnapshot AcquireSnapshot()
    {
        const int maxRetries = 64;

        for (int i = 0; i < maxRetries; i++)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

            var snap = _snapshot;
            if (snap.TryAddRef())
                return snap;
        }

        throw new InvalidOperationException(
            $"Failed to acquire MmapRegion snapshot after {maxRetries} retries. " +
            "This indicates extreme contention or a bug in snapshot lifecycle management.");
    }

    private MmapSnapshot CreateSnapshot(long size)
    {
        // Add a FileHandle ref BEFORE creating the snapshot.
        // If CreateFromFile throws, we release it in the catch.
        _fileHandle.AddRef();
        try
        {
            if (_fileHandle.Stream.Length < size)
                _fileHandle.Stream.SetLength(size);

            var mmap = MemoryMappedFile.CreateFromFile(
                _fileHandle.Stream,
                mapName: null,
                capacity: size,
                access: MemoryMappedFileAccess.ReadWrite,
                inheritability: HandleInheritability.None,
                leaveOpen: true);

            var accessor = mmap.CreateViewAccessor(0, size, MemoryMappedFileAccess.ReadWrite);
            return new MmapSnapshot(mmap, accessor, size, _fileHandle);
        }
        catch
        {
            _fileHandle.Release(); // rollback the AddRef on failure
            throw;
        }
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _disposed, 1, 0) != 0)
            return;

        // Release the current snapshot's owner reference.
        // This may or may not drop refCount to 0 depending on active readers.
        _snapshot?.Release();

        // Release MmapRegion's own reference to the SharedFileHandle.
        // FileStream closes ONLY when all snapshots have also released.
        _fileHandle.Release();
    }
}