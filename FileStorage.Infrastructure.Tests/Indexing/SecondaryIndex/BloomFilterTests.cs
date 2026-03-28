using FileStorage.Infrastructure.Indexing.SecondaryIndex;

namespace FileStorage.Infrastructure.Tests.Indexing.SecondaryIndex;

public sealed class BloomFilterTests
{
    [Fact]
    public void Create_HasNoFalseNegatives_ForInsertedKeys()
    {
        var keys = Enumerable.Range(0, 3000).Select(i => $"k{i}").ToArray();
        var bloom = BloomFilter.Create(keys, keys.Length);

        foreach (var key in keys)
            Assert.True(bloom.MayContain(key));
    }

    [Fact]
    public void SaveLoad_RoundTrip_PreservesMembershipBehavior()
    {
        string dir = CreateTempDir();
        string path = Path.Combine(dir, "test.bloom");

        try
        {
            var keys = Enumerable.Range(0, 1500).Select(i => $"key-{i}").ToArray();
            var bloom = BloomFilter.Create(keys, keys.Length);
            bloom.SaveTo(path);

            var loaded = BloomFilter.LoadFrom(path);

            foreach (var key in keys)
                Assert.True(loaded.MayContain(key));
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void FalsePositiveRate_StaysWithinReasonableBand()
    {
        var present = Enumerable.Range(0, 5000).Select(i => $"present-{i}").ToArray();
        var bloom = BloomFilter.Create(present, present.Length);

        int checkedAbsent = 5000;
        int falsePositives = 0;
        for (int i = 0; i < checkedAbsent; i++)
        {
            if (bloom.MayContain($"absent-{i}"))
                falsePositives++;
        }

        double rate = falsePositives / (double)checkedAbsent;
        Assert.InRange(rate, 0.0, 0.05);
    }

    [Fact]
    public void EmptyOrMissingFilter_IsAlwaysNegative()
    {
        var empty = BloomFilter.Create([], 0);
        Assert.False(empty.MayContain("x"));

        string dir = CreateTempDir();
        try
        {
            string missingPath = Path.Combine(dir, "missing.bloom");
            var loaded = BloomFilter.LoadFrom(missingPath);
            Assert.False(loaded.MayContain("x"));
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
