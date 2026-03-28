using System.Buffers.Binary;
using System.Text;
using FileStorage.Infrastructure.Core.Hashing;

namespace FileStorage.Infrastructure.WAL;

/// <summary>
/// Serializes and validates WAL batch payloads with Start/Commit markers and CRC32.
/// Payload layout:
/// [Start:4][Version:1][Count:4][Items...][Commit:4][BatchCrc32:4]
/// </summary>
internal static class WalBatchPayloadSerializer
{
    private const uint StartMarker = 0x54414257; // WBAT
    private const uint CommitMarker = 0x54494D43; // CMIT
    private const byte Version = 1;

    /// <summary>
    /// Serializes batch items into a WAL payload with commit marker and payload CRC32.
    /// </summary>
    public static byte[] Serialize(IReadOnlyCollection<WalBatchEntry> entries)
    {
        ArgumentNullException.ThrowIfNull(entries);
        if (entries.Count == 0)
            throw new ArgumentException("Batch cannot be empty.", nameof(entries));

        int totalSize = 4 + 1 + 4; // Start + Version + Count

        foreach (var entry in entries)
        {
            ArgumentNullException.ThrowIfNull(entry.Data);
            ArgumentNullException.ThrowIfNull(entry.IndexedFields);

            totalSize += 16 + 8 + 8 + 4 + 4 + entry.Data.Length;

            foreach (var (field, value) in entry.IndexedFields)
            {
                int fieldLen = Encoding.UTF8.GetByteCount(field);
                int valueLen = Encoding.UTF8.GetByteCount(value);
                totalSize += 4 + fieldLen + 4 + valueLen;
            }
        }

        totalSize += 4 + 4; // Commit + CRC32

        byte[] payload = new byte[totalSize];
        var span = payload.AsSpan();
        int pos = 0;

        BinaryPrimitives.WriteUInt32LittleEndian(span[pos..], StartMarker);
        pos += 4;

        span[pos++] = Version;

        BinaryPrimitives.WriteInt32LittleEndian(span[pos..], entries.Count);
        pos += 4;

        foreach (var entry in entries)
        {
            entry.Key.TryWriteBytes(span.Slice(pos, 16));
            pos += 16;

            BinaryPrimitives.WriteInt64LittleEndian(span[pos..], entry.DataOffset);
            pos += 8;

            BinaryPrimitives.WriteInt64LittleEndian(span[pos..], entry.IndexOffset);
            pos += 8;

            BinaryPrimitives.WriteInt32LittleEndian(span[pos..], entry.Data.Length);
            pos += 4;

            BinaryPrimitives.WriteInt32LittleEndian(span[pos..], entry.IndexedFields.Count);
            pos += 4;

            foreach (var (field, value) in entry.IndexedFields)
            {
                int fieldLen = Encoding.UTF8.GetByteCount(field);
                BinaryPrimitives.WriteInt32LittleEndian(span[pos..], fieldLen);
                pos += 4;
                Encoding.UTF8.GetBytes(field.AsSpan(), span.Slice(pos, fieldLen));
                pos += fieldLen;

                int valueLen = Encoding.UTF8.GetByteCount(value);
                BinaryPrimitives.WriteInt32LittleEndian(span[pos..], valueLen);
                pos += 4;
                Encoding.UTF8.GetBytes(value.AsSpan(), span.Slice(pos, valueLen));
                pos += valueLen;
            }

            entry.Data.CopyTo(span[pos..]);
            pos += entry.Data.Length;
        }

        BinaryPrimitives.WriteUInt32LittleEndian(span[pos..], CommitMarker);
        pos += 4;

        uint crc = Crc32.Compute(span[..pos]);
        BinaryPrimitives.WriteUInt32LittleEndian(span[pos..], crc);

        return payload;
    }

    /// <summary>
    /// Tries to parse and validate a WAL batch payload.
    /// Returns <c>false</c> for truncated/corrupted payloads or marker/CRC mismatches.
    /// </summary>
    public static bool TryDeserialize(ReadOnlySpan<byte> payload, out List<WalBatchEntry> entries)
    {
        entries = [];

        if (payload.Length < 4 + 1 + 4 + 4 + 4)
            return false;

        int pos = 0;

        uint start = BinaryPrimitives.ReadUInt32LittleEndian(payload[pos..]);
        pos += 4;
        if (start != StartMarker)
            return false;

        byte version = payload[pos++];
        if (version != Version)
            return false;

        int count = BinaryPrimitives.ReadInt32LittleEndian(payload[pos..]);
        pos += 4;
        if (count <= 0)
            return false;

        var result = new List<WalBatchEntry>(count);

        for (int i = 0; i < count; i++)
        {
            if (payload.Length - pos < 16 + 8 + 8 + 4 + 4)
                return false;

            Guid key = new(payload.Slice(pos, 16));
            pos += 16;

            long dataOffset = BinaryPrimitives.ReadInt64LittleEndian(payload[pos..]);
            pos += 8;

            long indexOffset = BinaryPrimitives.ReadInt64LittleEndian(payload[pos..]);
            pos += 8;

            int dataLen = BinaryPrimitives.ReadInt32LittleEndian(payload[pos..]);
            pos += 4;
            if (dataLen <= 0)
                return false;

            int fieldCount = BinaryPrimitives.ReadInt32LittleEndian(payload[pos..]);
            pos += 4;
            if (fieldCount < 0)
                return false;

            var indexedFields = new Dictionary<string, string>(fieldCount, StringComparer.Ordinal);

            for (int f = 0; f < fieldCount; f++)
            {
                if (payload.Length - pos < 4)
                    return false;
                int fieldLen = BinaryPrimitives.ReadInt32LittleEndian(payload[pos..]);
                pos += 4;
                if (fieldLen < 0 || payload.Length - pos < fieldLen)
                    return false;

                string field = Encoding.UTF8.GetString(payload.Slice(pos, fieldLen));
                pos += fieldLen;

                if (payload.Length - pos < 4)
                    return false;
                int valueLen = BinaryPrimitives.ReadInt32LittleEndian(payload[pos..]);
                pos += 4;
                if (valueLen < 0 || payload.Length - pos < valueLen)
                    return false;

                string value = Encoding.UTF8.GetString(payload.Slice(pos, valueLen));
                pos += valueLen;

                indexedFields[field] = value;
            }

            if (payload.Length - pos < dataLen)
                return false;

            byte[] data = payload.Slice(pos, dataLen).ToArray();
            pos += dataLen;

            result.Add(new WalBatchEntry(key, data, dataOffset, indexOffset, indexedFields));
        }

        if (payload.Length - pos < 8)
            return false;

        uint commit = BinaryPrimitives.ReadUInt32LittleEndian(payload[pos..]);
        pos += 4;
        if (commit != CommitMarker)
            return false;

        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(payload[pos..]);
        uint computed = Crc32.Compute(payload[..pos]);
        if (storedCrc != computed)
            return false;

        entries = result;
        return true;
    }
}
