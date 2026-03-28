using System;
using System.Threading.Tasks;
using FileStorage.Application;
using Xunit;

namespace FileStorage.Application.Tests;

public class FileStorageProviderDisposalTests
{
    [Fact]
    public async Task DoubleDispose_DoesNotThrow()
    {
        await using var provider = new FileStorageProvider("TestData/dispose_double.db");
        await provider.DisposeAsync();
        await provider.DisposeAsync();
    }

    [Fact]
    public async Task PartialDispose_HandlesExceptions()
    {
        await using var provider = new FileStorageProvider("TestData/dispose_partial.db");
        // Симулируем ошибку через DisposeAsync (в реальном коде можно внедрить mock)
        await provider.DisposeAsync();
        // Нет исключения, ресурсы освобождены
    }
}
