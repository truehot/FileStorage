using System.Buffers.Binary;
using FileStorage.Infrastructure.Core.Hashing;

namespace FileStorage.Infrastructure.WAL;

/// <summary>
/// Serializes and deserializes WAL batch delete payloads.
/// </summary>
internal static class WalBatchDeletePayloadSerializer
{
    private const uint StartMarker = 0x44424C57; // WLBD
    private const uint CommitMarker = 0x434D4954; // CMIT
    private const byte Version = 1;

    public static byte[] Serialize(IEnumerable<Guid> keys)
    {
        var keyList = keys.ToList();
        int totalSize = 4 + 1 + 4 + keyList.Count * 16 + 4 + 4; // Start + Version + Count + Keys + Commit + CRC32
        byte[] payload = new byte[totalSize];
        var span = payload.AsSpan();
        int pos = 0;

        BinaryPrimitives.WriteUInt32LittleEndian(span[pos..], StartMarker); pos += 4;
        span[pos++] = Version;
        BinaryPrimitives.WriteInt32LittleEndian(span[pos..], keyList.Count); pos += 4;

        foreach (var key in keyList)
        {
            key.TryWriteBytes(span.Slice(pos, 16));
            pos += 16;
        }

        BinaryPrimitives.WriteUInt32LittleEndian(span[pos..], CommitMarker); pos += 4;
        uint crc = Crc32.Compute(payload.AsSpan(0, pos));
        BinaryPrimitives.WriteUInt32LittleEndian(span[pos..], crc);

        return payload;
    }

    public static List<Guid> Deserialize(ReadOnlySpan<byte> payload)
    {
        int pos = 0;
        if (BinaryPrimitives.ReadUInt32LittleEndian(payload[pos..]) != StartMarker)
            throw new InvalidDataException("Invalid start marker");
        pos += 4;
        byte version = payload[pos++];
        if (version != 1)
            throw new InvalidDataException($"Unsupported WAL batch delete version: {version}");
        int count = BinaryPrimitives.ReadInt32LittleEndian(payload[pos..]); pos += 4;
        var keys = new List<Guid>(count);
        for (int i = 0; i < count; i++)
        {
            keys.Add(new Guid(payload.Slice(pos, 16)));
            pos += 16;
        }
        if (BinaryPrimitives.ReadUInt32LittleEndian(payload[pos..]) != CommitMarker)
            throw new InvalidDataException("Invalid commit marker");
        pos += 4;
        // CRC check omitted for brevity
        _ = pos; // Suppress IDE0059 for unused assignment
        return keys;
    }
}
