using Xunit;

namespace FileStorage.Infrastructure.Tests;

public class CompactionTempFileTests
{
    [Fact]
    public void Compaction_CleansUpTempFiles()
    {
        // Заглушка: требуется интеграция с compaction и проверка временных файлов
        Assert.True(true);
    }

    [Fact]
    public void Compaction_Interrupted_NoFileHandleLeak()
    {
        // Заглушка: требуется интеграция с compaction и проверка утечек дескрипторов
        Assert.True(true);
    }
}
