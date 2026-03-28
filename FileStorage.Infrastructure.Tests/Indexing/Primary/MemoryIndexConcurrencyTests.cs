using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FileStorage.Infrastructure.Indexing.Primary;
using Xunit;

namespace FileStorage.Infrastructure.Tests.Indexing.Primary
{
    public class MemoryIndexConcurrencyTests
    {
        [Fact]
        public async Task ParallelAddRemoveClear_DoesNotCorrupt()
        {
            var index = new MemoryIndex();
            var tasks = new List<Task>();
            for (int i = 0; i < 8; i++)
            {
                tasks.Add(Task.Run(() =>
                {
                    for (int j = 0; j < 1000; j++)
                    {
                        var key = Guid.NewGuid();
                        index.AddOrUpdate("t", key, j);
                        index.TryRemove("t", key);
                    }
                }));
            }
            await Task.WhenAll(tasks);
            index.Clear();
            Assert.Equal(0, index.Count);
        }
    }
}
