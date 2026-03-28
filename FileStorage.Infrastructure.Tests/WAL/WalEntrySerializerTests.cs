using System.Buffers.Binary;
using System.Text;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure.Tests.WAL;

public class WalEntrySerializerTests
{
    [Fact]
    public void Serialize_ReadBack_AllParts_AreConsistent()
    {
        var key = Guid.NewGuid();
        const long seqNo = 42;
        const long dataOffset = 12345;
        const long indexOffset = 67890;
        const string table = "users";
        byte[] data = Encoding.UTF8.GetBytes("{\"name\":\"alice\"}");

        var entry = new WalEntry
        {
            Operation = WalOperationType.Save,
            Table = table,
            Key = key,
            Data = data,
            DataOffset = dataOffset,
            IndexOffset = indexOffset,
            IndexedFields = new Dictionary<string, string>()
        };

        byte[] record = WalEntrySerializer.Serialize(entry, seqNo);

        // Header
        ReadOnlySpan<byte> header = record.AsSpan(0, WalEntrySerializer.MinHeaderSize);
        bool ok = WalEntrySerializer.TryReadHeader(header, out uint storedCrc, out long parsedSeqNo, out var op, out int tableLen);

        Assert.True(ok);
        Assert.Equal(seqNo, parsedSeqNo);
        Assert.Equal(WalOperationType.Save, op);
        Assert.Equal(Encoding.UTF8.GetByteCount(table), tableLen);

        // Variable part
        int varSize = WalEntrySerializer.VariablePartSize(tableLen);
        ReadOnlySpan<byte> varPart = record.AsSpan(WalEntrySerializer.MinHeaderSize, varSize);
        var (parsedTable, parsedKey, parsedDataLen) = WalEntrySerializer.ReadVariablePart(varPart, tableLen);

        Assert.Equal(table, parsedTable);
        Assert.Equal(key, parsedKey);
        Assert.Equal(data.Length, parsedDataLen);

        // Data and offsets
        int dataStart = WalEntrySerializer.MinHeaderSize + varSize;
        ReadOnlySpan<byte> dataPart = record.AsSpan(dataStart, parsedDataLen);
        int offsetsStart = dataStart + parsedDataLen;
        ReadOnlySpan<byte> offsetsPart = record.AsSpan(offsetsStart, WalEntrySerializer.OffsetTrailerSize);

        var (parsedDataOffset, parsedIndexOffset) = WalEntrySerializer.ReadOffsets(offsetsPart);
        Assert.Equal(dataOffset, parsedDataOffset);
        Assert.Equal(indexOffset, parsedIndexOffset);
        Assert.True(dataPart.SequenceEqual(data));

        // CRC: single-span and segmented variants
        Assert.True(WalEntrySerializer.VerifyCrc(record.AsSpan(4), storedCrc));
        Assert.True(WalEntrySerializer.VerifyCrc(
            header[4..],
            varPart,
            dataPart,
            offsetsPart,
            storedCrc));
    }

    [Fact]
    public void TryReadHeader_ReturnsFalse_WhenTableLenIsZero()
    {
        byte[] header = new byte[WalEntrySerializer.MinHeaderSize];
        header[4 + 8] = (byte)WalOperationType.Save; // Op
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(4 + 8 + 1), 0);

        bool ok = WalEntrySerializer.TryReadHeader(header, out _, out _, out _, out _);

        Assert.False(ok);
    }

    [Fact]
    public void TryReadHeader_ReturnsFalse_WhenTableLenIsTooLarge()
    {
        byte[] header = new byte[WalEntrySerializer.MinHeaderSize];
        header[4 + 8] = (byte)WalOperationType.Save; // Op
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(4 + 8 + 1), WalEntrySerializer.MaxTableLen + 1);

        bool ok = WalEntrySerializer.TryReadHeader(header, out _, out _, out _, out _);

        Assert.False(ok);
    }

    [Fact]
    public void VerifyCrc_ReturnsFalse_WhenPayloadIsMutated()
    {
        var entry = new WalEntry
        {
            Operation = WalOperationType.Save,
            Table = "users",
            Key = Guid.NewGuid(),
            Data = Encoding.UTF8.GetBytes("abc"),
            DataOffset = 1,
            IndexOffset = 2,
            IndexedFields = new Dictionary<string, string>()
        };

        byte[] record = WalEntrySerializer.Serialize(entry, seqNo: 1);
        uint storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(record.AsSpan(0, 4));

        // Corrupt one byte in payload region (after CRC field)
        record[10] ^= 0xFF;

        ReadOnlySpan<byte> payload = record.AsSpan(4);
        Assert.False(WalEntrySerializer.VerifyCrc(payload, storedCrc));
    }
}