using FileStorage.Infrastructure.Core.Serialization;
using System.Buffers.Binary;

namespace FileStorage.Infrastructure.Tests.Serialization;

public sealed class IndexEntrySerializerTests
{
    [Fact]
    public void Write_ThenReadBack_AllFields_AreConsistent()
    {
        Span<byte> buffer = stackalloc byte[IndexEntrySerializer.EntryFixedSize];

        string table = "users";
        Guid key = Guid.NewGuid();
        const long dataOffset = 1234;
        const int dataSize = 77;
        const long version = 9876;

        IndexEntrySerializer.Write(buffer, table, key, dataOffset, dataSize, version);

        Assert.False(IndexEntrySerializer.IsEmpty(buffer));
        Assert.False(IndexEntrySerializer.IsDeleted(buffer));
        Assert.Equal(table, IndexEntrySerializer.ReadTableName(buffer));
        Assert.True(IndexEntrySerializer.TableEquals(buffer, table));
        Assert.Equal(key, IndexEntrySerializer.ReadKey(buffer));
        Assert.Equal(dataOffset, IndexEntrySerializer.ReadDataOffset(buffer));
        Assert.Equal(dataSize, IndexEntrySerializer.ReadDataSize(buffer));
        Assert.Equal(version, IndexEntrySerializer.ReadVersion(buffer));
    }

    [Fact]
    public void MarkDeleted_SetsDeletedFlag()
    {
        byte[] buffer = new byte[IndexEntrySerializer.EntryFixedSize];
        IndexEntrySerializer.Write(buffer, "users", Guid.NewGuid(), 1, 1, 1);

        IndexEntrySerializer.MarkDeleted(buffer);

        Assert.True(IndexEntrySerializer.IsDeleted(buffer));
    }

    [Fact]
    public void ReadTableName_ReturnsEmpty_WhenTableLenCorrupted()
    {
        byte[] buffer = new byte[IndexEntrySerializer.EntryFixedSize];
        IndexEntrySerializer.Write(buffer, "users", Guid.NewGuid(), 1, 1, 1);

        // TableLenOffset in serializer is 9.
        BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(9), IndexEntrySerializer.MaxTableNameBytes + 10);

        string table = IndexEntrySerializer.ReadTableName(buffer);

        Assert.Equal(string.Empty, table);
    }

    [Fact]
    public void Write_Throws_WhenBufferTooSmall()
    {
        byte[] small = new byte[IndexEntrySerializer.EntryFixedSize - 1];

        Assert.Throws<ArgumentException>(() =>
            IndexEntrySerializer.Write(small, "users", Guid.NewGuid(), 1, 1, 1));
    }
}
