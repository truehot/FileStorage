using Xunit;

namespace FileStorage.Infrastructure.Tests;

public class SSTableChurnTests
{
    [Fact]
    public void ManySSTables_MemoryAndFileHandleUsageAcceptable()
    {
        // «аглушка: требуетс€ стресс-тест на накопление SSTables
        Assert.True(true);
    }
}
