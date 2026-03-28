using FileStorage.Infrastructure.Indexing.SecondaryIndex;

namespace FileStorage.Infrastructure.Tests.Indexing.SecondaryIndex;

public sealed class SSTableTests
{
    [Fact]
    public void WriteOpenLookup_RoundTripsUtf8KeysAndGuids()
    {
        string dir = CreateTempDir();
        string path = Path.Combine(dir, "sample.sst");

        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();

        try
        {
            var entries = new List<(string Key, List<Guid> Guids)>
            {
                ("alpha", [g1]),
                ("¸æ", [g2])
            };

            using var written = SSTable.Write(path, entries);
            using var opened = SSTable.Open(path);

            var alpha = opened.Lookup("alpha");
            var unicode = opened.Lookup("¸æ");

            Assert.Single(alpha);
            Assert.Contains(g1, alpha);

            Assert.Single(unicode);
            Assert.Contains(g2, unicode);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void Lookup_MissingKey_ReturnsEmpty()
    {
        string dir = CreateTempDir();
        string path = Path.Combine(dir, "sample.sst");

        try
        {
            using var sst = SSTable.Write(path, [
                ("a", new List<Guid> { Guid.NewGuid() }),
                ("b", new List<Guid> { Guid.NewGuid() })
            ]);

            Assert.Empty(sst.Lookup("z"));
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void Write_WithLargeEntryCrossingBlockBoundary_PreservesFollowingEntries()
    {
        string dir = CreateTempDir();
        string path = Path.Combine(dir, "large.sst");

        try
        {
            var largeGuids = new List<Guid>();
            for (int i = 0; i < 300; i++)
                largeGuids.Add(Guid.NewGuid());

            var lastGuid = Guid.NewGuid();

            using var sst = SSTable.Write(path, [
                ("large", largeGuids),
                ("tail", new List<Guid> { lastGuid })
            ]);

            var readLarge = sst.Lookup("large");
            var readTail = sst.Lookup("tail");

            Assert.Equal(largeGuids.Count, readLarge.Count);
            Assert.Single(readTail);
            Assert.Contains(lastGuid, readTail);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void Open_RebuildsSparseIndex_AndLookupWorksAcrossManyEntries()
    {
        string dir = CreateTempDir();
        string path = Path.Combine(dir, "many.sst");

        try
        {
            var entries = new List<(string Key, List<Guid> Guids)>();
            var map = new Dictionary<string, Guid>(StringComparer.Ordinal);

            for (int i = 0; i < 400; i++)
            {
                string key = $"k{i:D4}";
                var g = Guid.NewGuid();
                map[key] = g;
                entries.Add((key, [g]));
            }

            using (var written = SSTable.Write(path, entries))
            {
                Assert.NotEmpty(written.Sparse.Entries);
            }

            using var opened = SSTable.Open(path);
            Assert.NotEmpty(opened.Sparse.Entries);

            foreach (var key in new[] { "k0000", "k0123", "k0399" })
            {
                var lookup = opened.Lookup(key);
                Assert.Single(lookup);
                Assert.Contains(map[key], lookup);
            }
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
