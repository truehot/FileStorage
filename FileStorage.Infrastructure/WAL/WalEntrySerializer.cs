using System.Buffers.Binary;
using System.Text;
using FileStorage.Infrastructure.Core.Hashing;

namespace FileStorage.Infrastructure.WAL;

/// <summary>
/// Serializes and deserializes WAL entries to/from byte buffers.
/// Record layout: [CRC32:4][SeqNo:8][Op:1][TableLen:4][Table:N][Key:16][DataLen:4][Data:N][DataOffset:8][IndexOffset:8]
/// </summary>
internal static class WalEntrySerializer
{
    private const int CrcSize = 4;
    private const int SeqNoSize = 8;
    private const int OpSize = 1;
    private const int TableLenSize = 4;
    private const int GuidSize = 16;
    private const int DataLenSize = 4;
    private const int OffsetSize = 8;
    private const int IndexOffsetSize = 8;

    public const int FixedOverhead = CrcSize + SeqNoSize + OpSize + TableLenSize
                                   + GuidSize + DataLenSize + OffsetSize + IndexOffsetSize;

    public const int MinHeaderSize = CrcSize + SeqNoSize + OpSize + TableLenSize;
    public const int MaxTableLen = 256;

    /// <summary>
    /// Serializes a WAL entry into a byte array with CRC32 header.
    /// </summary>
    public static byte[] Serialize(WalEntry entry, long seqNo)
    {
        byte[] tableBytes = Encoding.UTF8.GetBytes(entry.Table);
        int dataLen = entry.Data?.Length ?? 0;
        int totalSize = FixedOverhead + tableBytes.Length + dataLen;

        byte[] record = new byte[totalSize];
        var span = record.AsSpan();

        int pos = CrcSize; // skip CRC placeholder

        BinaryPrimitives.WriteInt64LittleEndian(span[pos..], seqNo);
        pos += SeqNoSize;

        span[pos] = (byte)entry.Operation;
        pos += OpSize;

        BinaryPrimitives.WriteInt32LittleEndian(span[pos..], tableBytes.Length);
        pos += TableLenSize;

        tableBytes.CopyTo(span[pos..]);
        pos += tableBytes.Length;

        entry.Key.TryWriteBytes(span.Slice(pos, GuidSize));
        pos += GuidSize;

        BinaryPrimitives.WriteInt32LittleEndian(span[pos..], dataLen);
        pos += DataLenSize;

        if (dataLen > 0)
        {
            entry.Data.CopyTo(span[pos..]);
            pos += dataLen;
        }

        BinaryPrimitives.WriteInt64LittleEndian(span[pos..], entry.DataOffset);
        pos += OffsetSize;

        BinaryPrimitives.WriteInt64LittleEndian(span[pos..], entry.IndexOffset);

        // CRC32 over everything after CRC field
        uint crc = Crc32.Compute(record.AsSpan(CrcSize));
        BinaryPrimitives.WriteUInt32LittleEndian(span, crc);

        return record;
    }

    /// <summary>
    /// Parses the fixed header (CRC + SeqNo + Op + TableLen) from a buffer.
    /// Returns false if data is insufficient or obviously corrupted.
    /// </summary>
    public static bool TryReadHeader(ReadOnlySpan<byte> headerBuf, out uint crc, out long seqNo, out WalOperationType op, out int tableLen)
    {
        crc = BinaryPrimitives.ReadUInt32LittleEndian(headerBuf);
        seqNo = BinaryPrimitives.ReadInt64LittleEndian(headerBuf[CrcSize..]);
        op = (WalOperationType)headerBuf[CrcSize + SeqNoSize];
        tableLen = BinaryPrimitives.ReadInt32LittleEndian(headerBuf[(CrcSize + SeqNoSize + OpSize)..]);

        return tableLen > 0 && tableLen <= MaxTableLen;
    }

    /// <summary>
    /// Parses the variable portion of a record (table name, key, data length).
    /// </summary>
    public static (string Table, Guid Key, int DataLen) ReadVariablePart(ReadOnlySpan<byte> buf, int tableLen)
    {
        string table = Encoding.UTF8.GetString(buf[..tableLen]);
        Guid key = new(buf.Slice(tableLen, GuidSize));
        int dataLen = BinaryPrimitives.ReadInt32LittleEndian(buf[(tableLen + GuidSize)..]);
        return (table, key, dataLen);
    }

    /// <summary>
    /// Parses the offset pair from a buffer.
    /// </summary>
    public static (long DataOffset, long IndexOffset) ReadOffsets(ReadOnlySpan<byte> buf)
    {
        long dataOffset = BinaryPrimitives.ReadInt64LittleEndian(buf);
        long indexOffset = BinaryPrimitives.ReadInt64LittleEndian(buf[OffsetSize..]);
        return (dataOffset, indexOffset);
    }

    /// <summary>
    /// Verifies CRC32 of a payload against the stored value.
    /// </summary>
    public static bool VerifyCrc(ReadOnlySpan<byte> payload, uint storedCrc)
    {
        return Crc32.Compute(payload) == storedCrc;
    }

    /// <summary>
    /// Verifies CRC32 over multiple payload segments without concatenation.
    /// </summary>
    public static bool VerifyCrc(
        ReadOnlySpan<byte> part1,
        ReadOnlySpan<byte> part2,
        ReadOnlySpan<byte> part3,
        ReadOnlySpan<byte> part4,
        uint storedCrc)
    {
        return Crc32.Compute(part1, part2, part3, part4) == storedCrc;
    }
    /// <summary>
    /// Size of variable part after the fixed header: tableLen + Guid + DataLen field.
    /// </summary>
    public static int VariablePartSize(int tableLen) => tableLen + GuidSize + DataLenSize;

    /// <summary>
    /// Size of the offset trailer.
    /// </summary>
    public const int OffsetTrailerSize = OffsetSize + IndexOffsetSize;
}