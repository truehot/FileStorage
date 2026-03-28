using System;
using System.Threading.Tasks;
using FileStorage.Application;
using Xunit;

namespace FileStorage.Application.Tests;

public class FileStorageProviderOptionsValidationTests
{
    [Fact]
    public async Task FileStorageProvider_InvalidOptions_ThrowsFriendlyException()
    {
        var options = new FileStorageProviderOptions { FilePath = "" };
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await using var provider = new FileStorageProvider(options);
            await provider.GetAsync();
        });
    }
}
