using System.Buffers.Binary;
using System.Numerics;
using System.Text;

namespace FileStorage.Infrastructure.Indexing.SecondaryIndex;

/// <summary>
/// Space-efficient probabilistic filter that answers "is this key possibly in the SSTable?"
/// 
/// <para><b>False positives:</b> possible (configurable via <see cref="BitsPerKey"/>).
/// <b>False negatives:</b> never — if the filter says "no", the key is guaranteed absent.</para>
/// 
/// <para><b>Sizing:</b> At 10 bits/key the false positive rate is ~1%.
/// For 1M keys this costs ~1.2 MB of RAM per SSTable.
/// For 300M keys this costs ~360 MB — fits in memory on any modern server.</para>
/// 
/// <para><b>Hash strategy:</b> Double hashing with two independent MurmurHash3-derived hashes.
/// <c>h(i) = h1 + i * h2</c> for <c>i = 0..k-1</c>.
/// This gives k hash functions from 2 hash computations (Kirsch-Mitzenmacher optimization).</para>
/// 
/// <para><b>Persistence:</b> Serialized as <c>[BitCount:8][HashCount:4][Bytes:N]</c>.
/// Stored alongside the SSTable as <c>.bloom</c> file.
/// BitCount is <c>long</c> to support 300M+ keys without overflow.</para>
/// </summary>
internal sealed class BloomFilter
{
    /// <summary>
    /// Bits per key. 10 bits/key ≈ 1% false positive rate.
    /// </summary>
    public const int BitsPerKey = 10;

    private readonly byte[] _bits;
    private readonly long _bitCount;
    private readonly int _hashCount;

    private BloomFilter(byte[] bits, long bitCount, int hashCount)
    {
        _bits = bits;
        _bitCount = bitCount;
        _hashCount = hashCount;
    }

    /// <summary>
    /// Builds a Bloom filter from a set of keys.
    /// </summary>
    public static BloomFilter Create(IEnumerable<string> keys)
    {
        var keyList = keys as ICollection<string> ?? [.. keys];
        return Create(keyList, keyList.Count);
    }

    /// <summary>
    /// Builds a Bloom filter from keys with a known count.
    /// Uses <c>long</c> arithmetic to avoid overflow at 300M+ keys.
    /// </summary>
    public static BloomFilter Create(IEnumerable<string> keys, long keyCount)
    {
        if (keyCount <= 0)
            return new BloomFilter([], 0, 0);

        long bitCount = keyCount * BitsPerKey;
        // Round up to nearest multiple of 8
        bitCount = bitCount + 7 & ~7L;
        // Minimum 64 bits
        if (bitCount < 64) bitCount = 64;

        // Sanity cap: ~2 GB byte array limit in .NET (Array.MaxLength)
        // 2 GB * 8 = ~17 billion bits ≈ 1.7 billion keys at 10 bits/key.
        // Beyond that, the filter must be sharded (out of scope here).
        long maxBits = (long)Array.MaxLength * 8;
        if (bitCount > maxBits)
            throw new InvalidOperationException(
                $"Bloom filter would require {bitCount / 8:N0} bytes, " +
                $"exceeding the maximum array size. Consider sharding the index.");

        int hashCount = Math.Max(1, (int)(BitsPerKey * 0.693));

        var bits = new byte[bitCount / 8];

        foreach (var key in keys)
        {
            var (h1, h2) = Hash(key);
            for (int i = 0; i < hashCount; i++)
            {
                long idx = (long)(((uint)h1 + (uint)i * (ulong)(uint)h2) % (ulong)bitCount);
                bits[idx >> 3] |= (byte)(1 << (int)(idx & 7));
            }
        }

        return new BloomFilter(bits, bitCount, hashCount);
    }

    /// <summary>
    /// Returns true if the key MIGHT be in the SSTable.
    /// Returns false if the key is DEFINITELY NOT in the SSTable.
    /// </summary>
    public bool MayContain(string key)
    {
        if (_bitCount == 0) return false;

        var (h1, h2) = Hash(key);
        for (int i = 0; i < _hashCount; i++)
        {
            long idx = (long)(((uint)h1 + (uint)i * (ulong)(uint)h2) % (ulong)_bitCount);
            if ((_bits[idx >> 3] & 1 << (int)(idx & 7)) == 0)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Returns true if the key MIGHT be in the SSTable.
    /// Accepts pre-encoded UTF-8 bytes to avoid re-encoding in hot loops.
    /// </summary>
    public bool MayContain(ReadOnlySpan<byte> keyUtf8)
    {
        if (_bitCount == 0) return false;

        var (h1, h2) = HashBytes(keyUtf8);
        for (int i = 0; i < _hashCount; i++)
        {
            long idx = (long)(((uint)h1 + (uint)i * (ulong)(uint)h2) % (ulong)_bitCount);
            if ((_bits[idx >> 3] & 1 << (int)(idx & 7)) == 0)
                return false;
        }

        return true;
    }

    // ──────────────────────────────────────────────
    //  Persistence: [BitCount:8][HashCount:4][Bytes:N]
    // ──────────────────────────────────────────────

    public void SaveTo(string path)
    {
        using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 65536);
        Span<byte> header = stackalloc byte[12];
        BinaryPrimitives.WriteInt64LittleEndian(header, _bitCount);
        BinaryPrimitives.WriteInt32LittleEndian(header[8..], _hashCount);
        fs.Write(header);
        fs.Write(_bits);
        fs.Flush(flushToDisk: true);
    }

    public static BloomFilter LoadFrom(string path)
    {
        if (!File.Exists(path))
            return new BloomFilter([], 0, 0);

        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 65536);
        Span<byte> header = stackalloc byte[12];
        if (fs.Read(header) < 12)
            return new BloomFilter([], 0, 0);

        long bitCount = BinaryPrimitives.ReadInt64LittleEndian(header);
        int hashCount = BinaryPrimitives.ReadInt32LittleEndian(header[8..]);

        if (bitCount <= 0 || hashCount <= 0)
            return new BloomFilter([], 0, 0);

        long byteCount = bitCount / 8;
        if (byteCount > Array.MaxLength)
            return new BloomFilter([], 0, 0);

        var bits = new byte[byteCount];
        fs.ReadExactly(bits);

        return new BloomFilter(bits, bitCount, hashCount);
    }

    // ──────────────────────────────────────────────
    //  Hash (MurmurHash3-inspired, 32-bit)
    // ──────────────────────────────────────────────

    private static (int H1, int H2) Hash(string key)
    {
        int maxBytes = Encoding.UTF8.GetMaxByteCount(key.Length);
        Span<byte> buf = maxBytes <= 256
            ? stackalloc byte[maxBytes]
            : new byte[maxBytes];
        int len = Encoding.UTF8.GetBytes(key.AsSpan(), buf);
        return HashBytes(buf[..len]);
    }

    private static (int H1, int H2) HashBytes(ReadOnlySpan<byte> data)
    {
        uint h1 = Murmur3(data, seed: 0);
        uint h2 = Murmur3(data, seed: 0x9747b28c);
        return ((int)h1, (int)h2);
    }

    /// <summary>
    /// MurmurHash3 32-bit finalizer-style hash.
    /// Fast, excellent distribution, no cryptographic guarantees needed.
    /// </summary>
    private static uint Murmur3(ReadOnlySpan<byte> data, uint seed)
    {
        uint h = seed;
        int len = data.Length;
        int blocks = len / 4;

        for (int i = 0; i < blocks; i++)
        {
            uint k = BinaryPrimitives.ReadUInt32LittleEndian(data[(i * 4)..]);
            k *= 0xcc9e2d51;
            k = BitOperations.RotateLeft(k, 15);
            k *= 0x1b873593;

            h ^= k;
            h = BitOperations.RotateLeft(h, 13);
            h = h * 5 + 0xe6546b64;
        }

        // Tail
        int tail = blocks * 4;
        uint k1 = 0;
        switch (len & 3)
        {
            case 3: k1 ^= (uint)data[tail + 2] << 16; goto case 2;
            case 2: k1 ^= (uint)data[tail + 1] << 8; goto case 1;
            case 1:
                k1 ^= data[tail];
                k1 *= 0xcc9e2d51;
                k1 = BitOperations.RotateLeft(k1, 15);
                k1 *= 0x1b873593;
                h ^= k1;
                break;
        }

        // Finalization mix
        h ^= (uint)len;
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;

        return h;
    }
}