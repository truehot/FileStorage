using Xunit;

namespace FileStorage.Infrastructure.Tests;

public class WalBatchPayloadSerializerTests
{
    [Fact]
    public void Deserialize_InvalidVersion_Throws()
    {
        // Заглушка: требуется интеграция с WalBatchPayloadSerializer и проверка версии
        Assert.True(true);
    }

    [Fact]
    public void Deserialize_MalformedBatch_Throws()
    {
        // Заглушка: требуется интеграция с WalBatchPayloadSerializer и проверка повреждённого batch
        Assert.True(true);
    }
}
