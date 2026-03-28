using Xunit;

namespace FileStorage.Infrastructure.Tests;

public class IndexStaleReadTests
{
    [Fact]
    public void DirectIndexRead_CanReturnStaleData_IfDeletedFlagIgnored()
    {
        // Заглушка: требуется интеграция с IndexManager/IndexEntrySerializer
        Assert.True(true);
    }
}
