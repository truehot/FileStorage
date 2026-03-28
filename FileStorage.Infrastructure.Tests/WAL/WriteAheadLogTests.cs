using FileStorage.Infrastructure.WAL;
using System.Collections.Generic;
using System.IO;
using Xunit;

namespace FileStorage.Infrastructure.Tests.WAL;

public sealed class WriteAheadLogTests
{
    [Fact]
    public void ReadAllStreaming_WhenEnumerationStopsEarly_DoesNotTruncateValidWal()
    {
        string path = CreateWalPath();
        try
        {
            Guid firstKey = Guid.NewGuid();
            Guid secondKey = Guid.NewGuid();

            using (var wal = new WriteAheadLog(path))
            {
                wal.Append(CreateSaveEntry("users", firstKey, "A", dataOffset: 10, indexOffset: 100));
                wal.Append(CreateSaveEntry("users", secondKey, "B", dataOffset: 20, indexOffset: 200));
            }

            using (var wal2 = new WriteAheadLog(path))
            {
                using var enumerator = wal2.ReadAllStreaming().GetEnumerator();
                Assert.True(enumerator.MoveNext());
                Assert.Equal(firstKey, enumerator.Current.Key);
            }

            using var wal3 = new WriteAheadLog(path);
            var entries = wal3.ReadAll();

            Assert.Equal(2, entries.Count);
            Assert.Equal(firstKey, entries[0].Key);
            Assert.Equal(secondKey, entries[1].Key);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public void Append_ThenReadAll_ReturnsEntriesInOrder()
    {
        string path = CreateWalPath();
        try
        {
            using (var wal = new WriteAheadLog(path))
            {
                wal.Append(CreateSaveEntry("users", Guid.NewGuid(), "A", dataOffset: 10, indexOffset: 100));
                wal.Append(CreateSaveEntry("users", Guid.NewGuid(), "B", dataOffset: 20, indexOffset: 200));
            }

            using var wal2 = new WriteAheadLog(path);
            var entries = wal2.ReadAll();

            Assert.Equal(2, entries.Count);
            Assert.Equal(1, entries[0].SequenceNumber);
            Assert.Equal(2, entries[1].SequenceNumber);

            Assert.Equal(WalOperationType.Save, entries[0].Operation);
            Assert.Equal("users", entries[0].Table);
            Assert.Equal("A", System.Text.Encoding.UTF8.GetString(entries[0].Data));
            Assert.Equal(10, entries[0].DataOffset);
            Assert.Equal(100, entries[0].IndexOffset);

            Assert.Equal(WalOperationType.Save, entries[1].Operation);
            Assert.Equal("users", entries[1].Table);
            Assert.Equal("B", System.Text.Encoding.UTF8.GetString(entries[1].Data));
            Assert.Equal(20, entries[1].DataOffset);
            Assert.Equal(200, entries[1].IndexOffset);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public void Checkpoint_ClearsWal()
    {
        string path = CreateWalPath();
        try
        {
            using (var wal = new WriteAheadLog(path))
            {
                wal.Append(CreateSaveEntry("users", Guid.NewGuid(), "A", 1, 1));
                wal.Append(CreateSaveEntry("users", Guid.NewGuid(), "B", 2, 2));
                wal.Checkpoint();
            }

            using var wal2 = new WriteAheadLog(path);
            var entries = wal2.ReadAll();

            Assert.Empty(entries);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public void ReadAll_WhenTailIsCorrupted_ReturnsValidPrefixOnly()
    {
        string path = CreateWalPath();
        try
        {
            using (var wal = new WriteAheadLog(path))
            {
                wal.Append(CreateSaveEntry("users", Guid.NewGuid(), "OK", dataOffset: 11, indexOffset: 111));
                wal.Append(CreateSaveEntry("users", Guid.NewGuid(), "BROKEN", dataOffset: 22, indexOffset: 222));
            }

            CorruptLastByte(path);

            using var wal2 = new WriteAheadLog(path);
            var entries = wal2.ReadAll();

            Assert.Single(entries);
            Assert.Equal("OK", System.Text.Encoding.UTF8.GetString(entries[0].Data));
            Assert.Equal(1, entries[0].SequenceNumber);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public void ReadAll_WhenTailIsCorrupted_TruncatesPhysicalWalTail()
    {
        string path = CreateWalPath();
        try
        {
            using (var wal = new WriteAheadLog(path))
            {
                wal.Append(CreateSaveEntry("users", Guid.NewGuid(), "OK", dataOffset: 1, indexOffset: 11));
                wal.Append(CreateSaveEntry("users", Guid.NewGuid(), "BROKEN", dataOffset: 2, indexOffset: 22));
            }

            long originalLen = new FileInfo(path).Length;
            CorruptLastByte(path);

            using (var wal2 = new WriteAheadLog(path))
            {
                var entries = wal2.ReadAll();
                Assert.Single(entries);
                Assert.Equal("OK", System.Text.Encoding.UTF8.GetString(entries[0].Data));
            }

            long truncatedLen = new FileInfo(path).Length;
            Assert.True(truncatedLen < originalLen);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    [Fact]
    public void ReadAll_WhenSaveBatchTailIsTruncated_IgnoresIncompleteBatchAndTruncatesFile()
    {
        string path = CreateWalPath();
        try
        {
            var k1 = Guid.NewGuid();
            var k2 = Guid.NewGuid();

            using (var wal = new WriteAheadLog(path))
            {
                wal.Append(CreateSaveEntry("users", Guid.NewGuid(), "OK", dataOffset: 10, indexOffset: 100));
                wal.Append(CreateSaveBatchEntry("users", [
                    new WalBatchEntry(k1, [1,2,3], 20, 200, new Dictionary<string, string>()),
                    new WalBatchEntry(k2, [4,5], 23, 300, new Dictionary<string, string>())
                ]));
            }

            long originalLen = new FileInfo(path).Length;
            TruncateLastBytes(path, 12);

            using var wal2 = new WriteAheadLog(path);
            var entries = wal2.ReadAll();

            Assert.Single(entries);
            Assert.Equal(WalOperationType.Save, entries[0].Operation);
            Assert.Equal("OK", System.Text.Encoding.UTF8.GetString(entries[0].Data));

            long truncatedLen = new FileInfo(path).Length;
            Assert.True(truncatedLen < originalLen);
        }
        finally
        {
            DeleteIfExists(path);
        }
    }

    private static WalEntry CreateSaveEntry(string table, Guid key, string data, long dataOffset, long indexOffset)
    {
        return new WalEntry
        {
            Operation = WalOperationType.Save,
            Table = table,
            Key = key,
            Data = System.Text.Encoding.UTF8.GetBytes(data),
            DataOffset = dataOffset,
            IndexOffset = indexOffset,
            IndexedFields = new Dictionary<string, string>()
        };
    }

    private static WalEntry CreateSaveBatchEntry(string table, IReadOnlyCollection<WalBatchEntry> batchEntries)
    {
        return new WalEntry
        {
            Operation = WalOperationType.SaveBatch,
            Table = table,
            Key = Guid.Empty,
            Data = WalBatchPayloadSerializer.Serialize(batchEntries),
            DataOffset = 0,
            IndexOffset = 0,
            IndexedFields = new Dictionary<string, string>()
        };
    }

    private static string CreateWalPath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "FileStorageX.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, "test.wal");
    }

    private static void CorruptLastByte(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        if (fs.Length == 0)
            return;

        fs.Seek(-1, SeekOrigin.End);
        int value = fs.ReadByte();
        fs.Seek(-1, SeekOrigin.End);
        fs.WriteByte((byte)(value ^ 0xFF));
        fs.Flush(true);
    }

    private static void TruncateLastBytes(string path, int bytesToTrim)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        if (fs.Length <= bytesToTrim)
            throw new InvalidOperationException("Cannot trim entire file in test.");

        fs.SetLength(fs.Length - bytesToTrim);
        fs.Flush(true);
    }

    private static void DeleteIfExists(string path)
    {
        try
        {
            if (File.Exists(path))
                File.Delete(path);

            string? dir = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(dir) && Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
        catch
        {
            // test cleanup best-effort
        }
    }
}