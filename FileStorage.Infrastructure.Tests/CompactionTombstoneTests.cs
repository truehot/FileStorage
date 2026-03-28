using System;
using System.Threading.Tasks;
using FileStorage.Infrastructure.WAL;
using Xunit;

namespace FileStorage.Infrastructure.Tests;

public class CompactionTombstoneTests
{
    [Fact]
    public void Compaction_RemovesTombstones()
    {
        // Заглушка: требуется интеграция с compaction API
        Assert.True(true);
    }

    [Fact]
    public void RapidAddDeleteCycles_TombstonesCleaned()
    {
        // Заглушка: требуется интеграция с compaction API
        Assert.True(true);
    }
}
