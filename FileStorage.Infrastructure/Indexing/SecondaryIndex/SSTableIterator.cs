using System.Buffers;
using System.Buffers.Binary;
using System.Text;

namespace FileStorage.Infrastructure.Indexing.SecondaryIndex;

/// <summary>
/// Forward-only streaming iterator over an SSTable file.
/// Reads one entry at a time — constant memory regardless of file size.
/// Used by compaction to perform K-way merge without loading all data into RAM.
/// </summary>
internal sealed class SSTableIterator : IDisposable
{
    private readonly FileStream _stream;
    private readonly byte[] _keyBuf;
    private bool _disposed;

    public string? CurrentKey { get; private set; }
    public List<Guid>? CurrentGuids { get; private set; }
    public bool HasCurrent => CurrentKey is not null;

    private SSTableIterator(FileStream stream)
    {
        _stream = stream;
        _keyBuf = ArrayPool<byte>.Shared.Rent(SSTable.MaxKeyBytes);
    }

    public static SSTableIterator Open(string filePath)
    {
        var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 8192);
        var iter = new SSTableIterator(stream);
        iter.MoveNext(); // Position on first entry
        return iter;
    }

    /// <summary>
    /// Advances to the next entry. Returns false when EOF is reached.
    /// </summary>
    public bool MoveNext()
    {
        CurrentKey = null;
        CurrentGuids = null;

        Span<byte> header = stackalloc byte[4];

        while (_stream.Position < _stream.Length)
        {
            if (_stream.Read(header) < 4) return false;
            int keyLen = BinaryPrimitives.ReadInt32LittleEndian(header);

            // Zero padding — skip to next block
            if (keyLen == 0)
            {
                long blockStart = (_stream.Position - 4) / SSTable.BlockSize * SSTable.BlockSize;
                _stream.Position = blockStart + SSTable.BlockSize;
                continue;
            }

            if (keyLen < 0 || keyLen > SSTable.MaxKeyBytes) return false;
            if (_stream.Read(_keyBuf.AsSpan(0, keyLen)) < keyLen) return false;

            CurrentKey = Encoding.UTF8.GetString(_keyBuf, 0, keyLen);

            if (_stream.Read(header) < 4) return false;
            int guidCount = BinaryPrimitives.ReadInt32LittleEndian(header);

            CurrentGuids = new List<Guid>(guidCount);
            Span<byte> guidBuf = stackalloc byte[16];
            for (int i = 0; i < guidCount; i++)
            {
                if (_stream.Read(guidBuf) < 16) return false;
                CurrentGuids.Add(new Guid(guidBuf));
            }

            return true;
        }

        return false;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        ArrayPool<byte>.Shared.Return(_keyBuf);
        _stream.Dispose();
    }
}