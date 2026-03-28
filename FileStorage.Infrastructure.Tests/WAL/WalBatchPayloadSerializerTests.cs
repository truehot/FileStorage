using System.Buffers.Binary;
using System.Text;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure.Tests.WAL;

public sealed class WalBatchPayloadSerializerTests
{
    [Fact]
    public void Serialize_ThenTryDeserialize_Roundtrip_Succeeds()
    {
        var entries = new List<WalBatchEntry>
        {
            new(
                Guid.NewGuid(),
                Encoding.UTF8.GetBytes("alpha"),
                DataOffset: 10,
                IndexOffset: 100,
                new Dictionary<string, string> { ["category"] = "A", ["brand"] = "X" }),
            new(
                Guid.NewGuid(),
                Encoding.UTF8.GetBytes("beta"),
                DataOffset: 20,
                IndexOffset: 200,
                new Dictionary<string, string> { ["category"] = "B" })
        };

        byte[] payload = WalBatchPayloadSerializer.Serialize(entries);

        bool ok = WalBatchPayloadSerializer.TryDeserialize(payload, out var parsed);

        Assert.True(ok);
        Assert.Equal(2, parsed.Count);
        Assert.Equal(entries[0].Key, parsed[0].Key);
        Assert.Equal(entries[0].DataOffset, parsed[0].DataOffset);
        Assert.Equal(entries[0].IndexOffset, parsed[0].IndexOffset);
        Assert.Equal("alpha", Encoding.UTF8.GetString(parsed[0].Data));
        Assert.Equal("A", parsed[0].IndexedFields["category"]);
        Assert.Equal("X", parsed[0].IndexedFields["brand"]);

        Assert.Equal(entries[1].Key, parsed[1].Key);
        Assert.Equal("beta", Encoding.UTF8.GetString(parsed[1].Data));
        Assert.Equal("B", parsed[1].IndexedFields["category"]);
    }

    [Fact]
    public void TryDeserialize_ReturnsFalse_WhenCommitMarkerIsCorrupted()
    {
        var entries = new List<WalBatchEntry>
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes("payload"), 1, 2, new Dictionary<string, string>())
        };

        byte[] payload = WalBatchPayloadSerializer.Serialize(entries);

        int commitPos = payload.Length - 8; // [Commit:4][CRC:4]
        uint badCommit = 0xDEADBEEF;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(commitPos, 4), badCommit);

        bool ok = WalBatchPayloadSerializer.TryDeserialize(payload, out _);

        Assert.False(ok);
    }

    [Fact]
    public void TryDeserialize_ReturnsFalse_WhenStartMarkerIsCorrupted()
    {
        var entries = new List<WalBatchEntry>
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes("payload"), 1, 2, new Dictionary<string, string>())
        };

        byte[] payload = WalBatchPayloadSerializer.Serialize(entries);

        uint badStart = 0xABCD1234;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(0, 4), badStart);

        bool ok = WalBatchPayloadSerializer.TryDeserialize(payload, out _);

        Assert.False(ok);
    }

    [Fact]
    public void TryDeserialize_ReturnsFalse_WhenCrcIsCorrupted()
    {
        var entries = new List<WalBatchEntry>
        {
            new(Guid.NewGuid(), Encoding.UTF8.GetBytes("payload"), 1, 2, new Dictionary<string, string>())
        };

        byte[] payload = WalBatchPayloadSerializer.Serialize(entries);

        // Corrupt CRC at the end.
        int crcPos = payload.Length - 4;
        payload[crcPos] ^= 0xFF;

        bool ok = WalBatchPayloadSerializer.TryDeserialize(payload, out _);

        Assert.False(ok);
    }
}
