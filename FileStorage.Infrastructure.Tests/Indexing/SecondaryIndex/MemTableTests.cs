using FileStorage.Infrastructure.Indexing.SecondaryIndex;

namespace FileStorage.Infrastructure.Tests.Indexing.SecondaryIndex;

public sealed class MemTableTests
{
    [Fact]
    public void Put_DuplicateMapping_DeduplicatesAndKeepsTotalMappings()
    {
        var mem = new MemTable();
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();

        mem.Put("active", g1);
        mem.Put("active", g1);
        mem.Put("active", g2);

        Assert.Equal(1, mem.Count);
        Assert.Equal(2, mem.TotalMappings);

        var values = mem.Lookup("active");
        Assert.Equal(2, values.Count);
        Assert.Contains(g1, values);
        Assert.Contains(g2, values);
    }

    [Fact]
    public void Freeze_ReturnsSortedSnapshot_AndClearsMemTable()
    {
        var mem = new MemTable();
        var g1 = Guid.NewGuid();
        var g2 = Guid.NewGuid();

        mem.Put("b", g1);
        mem.Put("a", g2);

        var frozen = mem.Freeze();

        Assert.Equal(["a", "b"], [.. frozen.Keys]);
        Assert.Equal(0, mem.Count);
        Assert.Equal(0, mem.TotalMappings);
        Assert.Empty(mem.Lookup("a"));
        Assert.Empty(mem.Lookup("b"));
    }

    [Fact]
    public void RemoveByKey_RemovesKeyFromAllValues()
    {
        var mem = new MemTable();
        var keep = Guid.NewGuid();
        var remove = Guid.NewGuid();

        mem.Put("active", keep);
        mem.Put("active", remove);
        mem.Put("inactive", remove);

        mem.RemoveByKey(remove);

        var active = mem.Lookup("active");
        var inactive = mem.Lookup("inactive");

        Assert.Single(active);
        Assert.Contains(keep, active);
        Assert.DoesNotContain(remove, active);
        Assert.Empty(inactive);
    }
}
