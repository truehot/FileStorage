using System.Buffers;
using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace FileStorage.Infrastructure.Indexing.SecondaryIndex;

/// <summary>
/// Immutable sorted string table for secondary index data.
/// 
/// <para><b>Block-based format:</b></para>
/// Data is organized in 4 KB blocks. Each block contains packed entries:
/// <c>[KeyLen:4][Key:N][GuidCount:4][Guid1:16][Guid2:16]...</c>
/// Blocks are padded to 4 KB boundaries with zeroes.
/// 
/// <para><b>Bloom filter:</b></para>
/// Each SSTable has an associated <c>.bloom</c> file loaded at open time.
/// <see cref="Lookup"/> checks the Bloom filter first — if the key is definitely absent,
/// the sparse index and MMF are never touched. At 10 bits/key, false positive rate ≈ 1%.
/// 
/// <para><b>Sparse index:</b></para>
/// Every <see cref="SparseInterval"/>-th key is recorded with its block-aligned offset.
/// This enables binary search → jump to block → linear scan within block.
/// 
/// <para><b>Memory-mapped reads:</b></para>
/// Uses <see cref="MemoryMappedFile"/> with <see cref="MemoryMappedViewStream"/> for lookups.
/// No per-Lookup file handle allocation. Key comparison via <c>Span&lt;byte&gt;</c>
/// using <see cref="MemoryMappedViewStream.Read(Span{byte})"/> — avoids ReadArray overhead.
/// </summary>
internal sealed class SSTable : IDisposable
{
    public const int SparseInterval = 64;
    public const int BlockSize = 4096;
    internal const int MaxKeyBytes = 1024;
    private const int MmfOpenMaxRetries = 3;
    private const int MmfOpenRetryDelayMs = 50;

    public string FilePath { get; }
    public SparseIndex Sparse { get; }

    private readonly BloomFilter _bloom;
    private readonly MemoryMappedFile? _mmf;
    private readonly long _fileLength;
    private bool _disposed;

    private SSTable(string filePath, SparseIndex sparse, BloomFilter bloom, MemoryMappedFile? mmf, long fileLength)
    {
        FilePath = filePath;
        Sparse = sparse;
        _bloom = bloom;
        _mmf = mmf;
        _fileLength = fileLength;
    }

    // ──────────────────────────────────────────────
    //  Write (block-aligned + Bloom filter)
    // ──────────────────────────────────────────────

    /// <summary>
    /// Writes entries from an <see cref="IEnumerable{T}"/> of sorted (key, guids) pairs
    /// to a block-aligned SSTable file. Builds Bloom filter and sparse index during write.
    /// </summary>
    public static SSTable Write(string filePath, IEnumerable<(string Key, List<Guid> Guids)> sortedEntries)
    {
        var sparseEntries = new List<SparseIndexEntry>();
        var bloomKeys = new List<string>();
        Span<byte> paddingBuf = stackalloc byte[BlockSize];
        paddingBuf.Clear();

        using (var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write, FileShare.None, 65536))
        {
            int entryIndex = 0;
            long blockStart = 0;
            int blockUsed = 0;

            foreach (var (key, guids) in sortedEntries)
            {
                bloomKeys.Add(key);

                int entrySize = MeasureEntry(key, guids.Count);

                if (blockUsed > 0 && blockUsed + entrySize > BlockSize)
                {
                    int padding = BlockSize - blockUsed;
                    if (padding > 0)
                        fs.Write(paddingBuf[..padding]);
                    blockStart = fs.Position;
                    blockUsed = 0;
                }

                if (blockUsed == 0 && entryIndex % SparseInterval == 0)
                    sparseEntries.Add(new SparseIndexEntry(key, blockStart));

                WriteEntry(fs, key, guids);
                blockUsed += entrySize;

                // If the entry spanned beyond the block, align to the next block boundary
                if (blockUsed > BlockSize)
                {
                    int overflowPadding = BlockSize - (int)(fs.Position % BlockSize);
                    if (overflowPadding > 0 && overflowPadding < BlockSize)
                        fs.Write(paddingBuf[..overflowPadding]);
                    blockStart = fs.Position;
                    blockUsed = 0;
                }
                entryIndex++;
            }

            if (blockUsed > 0)
            {
                int padding = BlockSize - blockUsed;
                if (padding > 0)
                    fs.Write(paddingBuf[..padding]);
            }
        }

        // Build and persist Bloom filter alongside .sst
        var bloom = BloomFilter.Create(bloomKeys, bloomKeys.Count);
        bloom.SaveTo(BloomPath(filePath));

        return OpenMappedFile(filePath, sparseEntries, bloom);
    }

    /// <summary>
    /// Writes a frozen MemTable snapshot to a block-aligned SSTable file.
    /// </summary>
    public static SSTable Write(string filePath, SortedDictionary<string, List<Guid>> data)
    {
        return Write(filePath, EnumerateDict(data));

        static IEnumerable<(string Key, List<Guid> Guids)> EnumerateDict(SortedDictionary<string, List<Guid>> d)
        {
            foreach (var (key, guids) in d)
                yield return (key, guids);
        }
    }

    // ──────────────────────────────────────────────
    //  Open (rebuild sparse index + load Bloom)
    // ──────────────────────────────────────────────

    /// <summary>
    /// Opens an existing SSTable file, rebuilds the sparse index, loads the Bloom filter
    /// from the <c>.bloom</c> sidecar file, and memory-maps the data file.
    /// </summary>
    public static SSTable Open(string filePath)
    {
        var info = new FileInfo(filePath);
        if (!info.Exists || info.Length == 0)
            return new SSTable(filePath, new SparseIndex([]), BloomFilter.Create([], 0), null, 0);

        // Load Bloom filter from .bloom sidecar
        var bloom = BloomFilter.LoadFrom(BloomPath(filePath));

        var sparseEntries = new List<SparseIndexEntry>();
        long fileLength = info.Length;

        var mmf = OpenMmfWithRetry(filePath);
        using var stream = mmf.CreateViewStream(0, fileLength, MemoryMappedFileAccess.Read);

        Span<byte> header = stackalloc byte[4];
        byte[] keyBuf = ArrayPool<byte>.Shared.Rent(MaxKeyBytes);
        try
        {
            long pos = 0;
            int entryIndex = 0;

            while (pos + 4 <= fileLength)
            {
                long blockStart = pos / BlockSize * BlockSize;

                stream.Position = pos;
                if (stream.Read(header) < 4) break;
                int keyLen = BinaryPrimitives.ReadInt32LittleEndian(header);

                if (keyLen == 0)
                {
                    pos = blockStart + BlockSize;
                    continue;
                }

                if (keyLen < 0 || keyLen > MaxKeyBytes || pos + 4 + keyLen + 4 > fileLength)
                    break;

                if (entryIndex % SparseInterval == 0)
                {
                    if (stream.Read(keyBuf.AsSpan(0, keyLen)) < keyLen) break;
                    string sparseKey = Encoding.UTF8.GetString(keyBuf, 0, keyLen);
                    sparseEntries.Add(new SparseIndexEntry(sparseKey, blockStart));
                }

                long guidCountPos = pos + 4 + keyLen;
                stream.Position = guidCountPos;
                if (stream.Read(header) < 4) break;
                int guidCount = BinaryPrimitives.ReadInt32LittleEndian(header);

                pos = guidCountPos + 4 + guidCount * 16L;
                entryIndex++;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(keyBuf);
        }

        return new SSTable(filePath, new SparseIndex(sparseEntries), bloom, mmf, fileLength);
    }

    // ──────────────────────────────────────────────
    //  Lookup (Bloom → Sparse → MMF scan)
    // ──────────────────────────────────────────────

    /// <summary>
    /// Looks up all record Guids for an exact field value.
    /// 
    /// <para><b>Lookup pipeline:</b></para>
    /// <list type="number">
    ///   <item><b>Bloom filter</b> — O(1), in-memory. If negative → return empty immediately.</item>
    ///   <item><b>Sparse index</b> — O(log N) binary search to find the starting block.</item>
    ///   <item><b>Block scan</b> — sequential scan within the block via MMF.</item>
    /// </list>
    /// </summary>
    public List<Guid> Lookup(string value)
    {
        if (_mmf is null || _fileLength == 0)
            return [];

        // Encode value to UTF-8 once — reused by both Bloom and key comparison
        int maxValueBytes = Encoding.UTF8.GetMaxByteCount(value.Length);
        byte[]? rentedValueBuf = null;
        Span<byte> valueBytes = maxValueBytes <= 256
            ? stackalloc byte[maxValueBytes]
            : (rentedValueBuf = ArrayPool<byte>.Shared.Rent(maxValueBytes));

        int valueBytesLen = Encoding.UTF8.GetBytes(value.AsSpan(), valueBytes);
        ReadOnlySpan<byte> valueUtf8 = valueBytes[..valueBytesLen];

        try
        {
            // ── Step 1: Bloom filter ──
            if (!_bloom.MayContain(valueUtf8))
                return [];

            // ── Step 2: Sparse index → starting block ──
            long startOffset = Sparse.FindStartOffset(value);

            // ── Step 3: Block scan via MMF ──
            byte[] keyBuf = ArrayPool<byte>.Shared.Rent(MaxKeyBytes);
            try
            {
                using var stream = _mmf.CreateViewStream(0, _fileLength, MemoryMappedFileAccess.Read);
                Span<byte> header = stackalloc byte[4];
                long pos = startOffset;

                while (pos + 4 <= _fileLength)
                {
                    stream.Position = pos;
                    if (stream.Read(header) < 4) break;
                    int keyLen = BinaryPrimitives.ReadInt32LittleEndian(header);

                    if (keyLen == 0)
                    {
                        pos = (pos / BlockSize + 1) * BlockSize;
                        continue;
                    }

                    if (keyLen < 0 || keyLen > MaxKeyBytes || pos + 4 + keyLen + 4 > _fileLength)
                        break;

                    if (stream.Read(keyBuf.AsSpan(0, keyLen)) < keyLen) break;
                    ReadOnlySpan<byte> storedKey = keyBuf.AsSpan(0, keyLen);

                    if (stream.Read(header) < 4) break;
                    int guidCount = BinaryPrimitives.ReadInt32LittleEndian(header);

                    int cmp = storedKey.SequenceCompareTo(valueUtf8);

                    if (cmp == 0)
                        return ReadGuids(stream, guidCount);

                    if (cmp > 0)
                        break;

                    stream.Position += guidCount * 16L;
                    pos = stream.Position;
                }

                return [];
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(keyBuf);
            }
        }
        finally
        {
            if (rentedValueBuf is not null)
                ArrayPool<byte>.Shared.Return(rentedValueBuf);
        }
    }

    // ──────────────────────────────────────────────
    //  Lifecycle
    // ──────────────────────────────────────────────

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _mmf?.Dispose();
    }

    public void DeleteFile()
    {
        Dispose();
        TryDeleteFile(FilePath);
        TryDeleteFile(BloomPath(FilePath));
    }

    // ──────────────────────────────────────────────
    //  Private helpers
    // ──────────────────────────────────────────────

    private static List<Guid> ReadGuids(MemoryMappedViewStream stream, int count)
    {
        var result = new List<Guid>(count);
        Span<byte> guidBuf = stackalloc byte[16];

        for (int i = 0; i < count; i++)
        {
            if (stream.Read(guidBuf) < 16) break;
            result.Add(new Guid(guidBuf));
        }

        return result;
    }

    private static SSTable OpenMappedFile(string filePath, List<SparseIndexEntry> sparseEntries, BloomFilter bloom)
    {
        var info = new FileInfo(filePath);
        if (info.Length == 0)
            return new SSTable(filePath, new SparseIndex(sparseEntries), bloom, null, 0);

        var mmf = OpenMmfWithRetry(filePath);
        return new SSTable(filePath, new SparseIndex(sparseEntries), bloom, mmf, info.Length);
    }

    private static MemoryMappedFile OpenMmfWithRetry(string filePath)
    {
        for (int attempt = 1; ; attempt++)
        {
            try
            {
                return MemoryMappedFile.CreateFromFile(
                    filePath, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
            }
            catch (IOException) when (attempt < MmfOpenMaxRetries)
            {
                Thread.Sleep(MmfOpenRetryDelayMs * attempt);
            }
        }
    }

    private static string BloomPath(string sstPath) => Path.ChangeExtension(sstPath, ".bloom");

    private static void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch { /* best effort */ }
    }

    private static int MeasureEntry(string key, int guidCount)
    {
        int keyBytes = Encoding.UTF8.GetByteCount(key);
        return 4 + keyBytes + 4 + guidCount * 16;
    }

    private static void WriteEntry(FileStream fs, string key, List<Guid> guids)
    {
        byte[] keyBytes = Encoding.UTF8.GetBytes(key);

        Span<byte> header = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(header, keyBytes.Length);
        fs.Write(header);
        fs.Write(keyBytes);

        BinaryPrimitives.WriteInt32LittleEndian(header, guids.Count);
        fs.Write(header);

        Span<byte> guidBuf = stackalloc byte[16];
        foreach (var guid in guids)
        {
            guid.TryWriteBytes(guidBuf);
            fs.Write(guidBuf);
        }
    }
}