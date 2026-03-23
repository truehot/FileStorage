namespace FileStorage.Infrastructure.IO;

/// <summary>
/// Defines the contract for a memory-mapped file region supporting read, write, growth, and flush operations.
/// </summary>
internal interface IMmapRegion : IDisposable
{
    string Path { get; }
    long FileSize { get; }
    void EnsureCapacity(long writePos, long required);
    void Read(long offset, byte[] buffer, int bufferOffset, int count);
    void Write(long offset, byte[] buffer, int bufferOffset, int count);
    void Flush();
}