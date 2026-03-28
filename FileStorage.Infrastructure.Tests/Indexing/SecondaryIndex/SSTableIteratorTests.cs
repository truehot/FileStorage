using FileStorage.Infrastructure.Indexing.SecondaryIndex;

namespace FileStorage.Infrastructure.Tests.Indexing.SecondaryIndex;

public sealed class SSTableIteratorTests
{
    [Fact]
    public void Iterator_ReadsAllEntriesInOrder_AndSkipsPadding()
    {
        string dir = CreateTempDir();
        string path = Path.Combine(dir, "iter.sst");

        try
        {
            var entries = new List<(string Key, List<Guid> Guids)>();
            for (int i = 0; i < 180; i++)
                entries.Add(($"k{i:D4}", [Guid.NewGuid()]));

            using var sst = SSTable.Write(path, entries);

            var readKeys = new List<string>();
            using var iter = SSTableIterator.Open(path);
            while (iter.HasCurrent)
            {
                readKeys.Add(iter.CurrentKey!);
                iter.MoveNext();
            }

            Assert.Equal(entries.Count, readKeys.Count);
            Assert.Equal(entries.Select(e => e.Key), readKeys);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        string dir = CreateTempDir();
        string path = Path.Combine(dir, "iter.sst");

        try
        {
            using var sst = SSTable.Write(path, [
                ("a", new List<Guid> { Guid.NewGuid() })
            ]);

            var iter = SSTableIterator.Open(path);
            iter.Dispose();
            iter.Dispose();
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    private static string CreateTempDir()
    {
        string dir = Path.Combine(Path.GetTempPath(), "FileStorageX.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void TryDeleteDir(string dir)
    {
        try
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
        catch
        {
        }
    }
}
