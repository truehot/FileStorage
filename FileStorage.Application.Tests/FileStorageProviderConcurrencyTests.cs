using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FileStorage.Application;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace FileStorage.Application.Tests
{
    public class FileStorageProviderConcurrencyTests
    {
        [Fact]
        public async Task DisposeAsync_ConcurrentWithGetAsync_ShouldNotLeakOrRace()
        {
            var provider = new FileStorageProvider("TestData/concurrent.db", NullLogger<FileStorageProvider>.Instance);
            var db = await provider.GetAsync();
            var cts = new CancellationTokenSource();
            var tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    try { await provider.GetAsync(cts.Token); } catch (ObjectDisposedException) { }
                }));
            }
            await provider.DisposeAsync();
            cts.Cancel();
            await Task.WhenAll(tasks);
            await Assert.ThrowsAsync<ObjectDisposedException>(() => provider.GetAsync());
        }
    }
}
