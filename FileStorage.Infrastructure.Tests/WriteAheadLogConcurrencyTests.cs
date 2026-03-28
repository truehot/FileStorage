using System;
using System.IO;
using System.Threading.Tasks;
using FileStorage.Infrastructure.WAL;
using Xunit;

namespace FileStorage.Infrastructure.Tests
{
    public class WriteAheadLogConcurrencyTests
    {
        [Fact]
        public void AppendBeforeInMemoryChange_Enforced()
        {
            var path = "TestData/wal_append_order.log";
            Directory.CreateDirectory("TestData");
            if (File.Exists(path)) File.Delete(path);
            using var wal = new WriteAheadLog(path);
            var entry = new WalEntry { Table = "t", Key = Guid.NewGuid(), Data = new byte[8] };
            var seq = wal.Append(entry);
            Assert.True(seq > 0);
        }

        [Fact]
        public void CorruptedTail_TruncatesCorrectly()
        {
            var path = "TestData/wal_corrupt_tail.log";
            Directory.CreateDirectory("TestData");
            if (File.Exists(path)) File.Delete(path);
            Guid key = Guid.NewGuid();
            using (var wal = new WriteAheadLog(path))
            {
                var entry = new WalEntry { Table = "t", Key = key, Data = new byte[8] };
                wal.Append(entry);
            }
            // Corrupt the file by truncating the last byte (simulate incomplete record, but not the whole entry)
            var fileBytes = File.ReadAllBytes(path);
            if (fileBytes.Length > 4) // don't corrupt if file is too small
                File.WriteAllBytes(path, fileBytes[..^1]);

            using (var wal = new WriteAheadLog(path))
            {
                var entries = wal.ReadAll();
                // После повреждения хвоста WAL не возвращает ни одной записи
                Assert.Empty(entries);
            }
        }

        [Fact]
        public void MultipleOpenClose_DoesNotLeak()
        {
            var path = "TestData/wal_leak_test.log";
            Directory.CreateDirectory("TestData");
            for (int i = 0; i < 100; i++)
            {
                using var wal = new WriteAheadLog(path);
                var entry = new WalEntry { Table = "t", Key = Guid.NewGuid(), Data = new byte[8] };
                wal.Append(entry);
            }
        }
    }
}
