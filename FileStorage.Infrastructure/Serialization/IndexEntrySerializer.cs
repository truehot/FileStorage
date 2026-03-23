using System.Buffers.Binary;
using System.Text;

namespace FileStorage.Infrastructure.Serialization;

/// <summary>
/// Serializes and deserializes fixed-size index entries to/from byte buffers.
/// Layout: [IsDeleted:1][DeletedAt:8][TableNameLen:4][TableName:256][Key:16][DataOffset:8][DataSize:4][Version:8]
/// </summary>
internal static class IndexEntrySerializer
{
    public const int MaxTableNameBytes = 256;
    public const int GuidSize = 16;

    public const int EntryFixedSize = sizeof(bool) + sizeof(long)
                                    + sizeof(int) + MaxTableNameBytes
                                    + GuidSize + sizeof(long) + sizeof(int) + sizeof(long);

    private const int TableLenOffset = 9;
    private const int TableDataOffset = 13;
    private const int GuidOffset = TableDataOffset + MaxTableNameBytes;
    private const int DataOffsetPos = GuidOffset + GuidSize;
    private const int DataSizePos = DataOffsetPos + sizeof(long);
    private const int VersionPos = DataSizePos + sizeof(int);

    public static void Write(Span<byte> buffer, string table, Guid key, long dataOffset, int dataSize, long version)
    {
        buffer.Slice(0, EntryFixedSize).Clear();

        int tableByteCount = Encoding.UTF8.GetBytes(table.AsSpan(), buffer.Slice(TableDataOffset, MaxTableNameBytes));
        int len = Math.Min(tableByteCount, MaxTableNameBytes - 1);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(TableLenOffset), len);

        key.TryWriteBytes(buffer.Slice(GuidOffset, GuidSize));

        BinaryPrimitives.WriteInt64LittleEndian(buffer.Slice(DataOffsetPos), dataOffset);
        BinaryPrimitives.WriteInt32LittleEndian(buffer.Slice(DataSizePos), dataSize);
        BinaryPrimitives.WriteInt64LittleEndian(buffer.Slice(VersionPos), version);
    }

    public static bool IsEmpty(ReadOnlySpan<byte> buffer)
    {
        for (int i = 0; i < EntryFixedSize; i++)
        {
            if (buffer[i] != 0) return false;
        }
        return true;
    }

    public static bool IsDeleted(ReadOnlySpan<byte> buffer) => buffer[0] != 0;

    public static void MarkDeleted(Span<byte> buffer) => buffer[0] = 1;

    public static Guid ReadKey(ReadOnlySpan<byte> buffer) =>
        new Guid(buffer.Slice(GuidOffset, GuidSize));

    public static long ReadDataOffset(ReadOnlySpan<byte> buffer) =>
        BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(DataOffsetPos));

    public static int ReadDataSize(ReadOnlySpan<byte> buffer) =>
        BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(DataSizePos));

    public static long ReadVersion(ReadOnlySpan<byte> buffer) =>
        BinaryPrimitives.ReadInt64LittleEndian(buffer.Slice(VersionPos));

    public static string ReadTableName(ReadOnlySpan<byte> buffer)
    {
        int tableLen = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(TableLenOffset));
        if (tableLen <= 0 || tableLen > MaxTableNameBytes) return string.Empty;
        return Encoding.UTF8.GetString(buffer.Slice(TableDataOffset, tableLen));
    }

    public static bool TableEquals(ReadOnlySpan<byte> buffer, string table)
    {
        int tableLen = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(TableLenOffset));
        if (tableLen <= 0 || tableLen > MaxTableNameBytes) return false;

        ReadOnlySpan<byte> storedBytes = buffer.Slice(TableDataOffset, tableLen);

        Span<byte> tableBytes = stackalloc byte[MaxTableNameBytes];
        int written = Encoding.UTF8.GetBytes(table.AsSpan(), tableBytes);

        return storedBytes.SequenceEqual(tableBytes.Slice(0, written));
    }
}