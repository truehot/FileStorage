using FileStorage.Infrastructure.Indexing.SecondaryIndex;

namespace FileStorage.Infrastructure.Tests.Indexing.SecondaryIndex;

public sealed class SparseIndexTests
{
    [Fact]
    public void FindStartOffset_ReturnsNearestLowerOrEqualBlock()
    {
        var index = new SparseIndex([
            new SparseIndexEntry("apple", 0),
            new SparseIndexEntry("banana", 4096),
            new SparseIndexEntry("cherry", 8192)
        ]);

        Assert.Equal(0, index.FindStartOffset("apple"));
        Assert.Equal(4096, index.FindStartOffset("banana"));
        Assert.Equal(4096, index.FindStartOffset("blueberry"));
        Assert.Equal(8192, index.FindStartOffset("cherry"));
    }

    [Fact]
    public void FindStartOffset_ReturnsZero_WhenKeyIsBeforeFirstOrNoEntries()
    {
        var index = new SparseIndex([
            new SparseIndexEntry("banana", 4096),
            new SparseIndexEntry("cherry", 8192)
        ]);

        Assert.Equal(0, index.FindStartOffset("aardvark"));

        var empty = new SparseIndex([]);
        Assert.Equal(0, empty.FindStartOffset("anything"));
    }
}
