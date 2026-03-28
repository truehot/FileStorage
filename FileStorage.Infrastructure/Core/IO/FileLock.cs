namespace FileStorage.Infrastructure.Core.IO;

/// <summary>
/// Provides cross-process exclusive access to a storage directory
/// by holding an open lock file with <see cref="FileShare.None"/>.
/// The lock is released when this instance is disposed.
/// </summary>
internal sealed class FileLock : IDisposable
{
    private FileStream? _lockStream;

    public string LockFilePath { get; }

    private FileLock(FileStream lockStream, string lockFilePath)
    {
        _lockStream = lockStream;
        LockFilePath = lockFilePath;
    }

    /// <summary>
    /// Acquires an exclusive file lock for the given storage path.
    /// Throws <see cref="IOException"/> if another process already holds the lock.
    /// </summary>
    public static FileLock Acquire(string storagePath)
    {
        var dir = Path.GetDirectoryName(storagePath);
        var name = Path.GetFileNameWithoutExtension(storagePath);
        var lockFilePath = Path.Combine(dir ?? ".", $"{name}.lock");

        FileStream stream;
        try
        {
            stream = new FileStream(
                lockFilePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None,
                bufferSize: 1,
                FileOptions.DeleteOnClose);
        }
        catch (IOException ex)
        {
            throw new IOException(
                $"Storage '{storagePath}' is already in use by another process. " +
                $"Lock file: '{lockFilePath}'.", ex);
        }

        // Write a small marker so it's clear what owns the lock.
        var info = System.Text.Encoding.UTF8.GetBytes(
            $"PID={Environment.ProcessId}, Acquired={DateTime.UtcNow:O}");
        stream.Write(info);
        stream.Flush();

        return new FileLock(stream, lockFilePath);
    }

    public void Dispose()
    {
        Interlocked.Exchange(ref _lockStream, null)?.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Safety net: if Dispose was never called (e.g. OOM during initialization),
    /// the finalizer ensures the OS file handle is released.
    /// </summary>
    ~FileLock()
    {
        Interlocked.Exchange(ref _lockStream, null)?.Dispose();
    }
}