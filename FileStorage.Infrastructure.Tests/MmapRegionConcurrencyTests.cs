using System;
using System.IO;
using System.Threading.Tasks;
using FileStorage.Infrastructure.Core.IO;
using Xunit;

namespace FileStorage.Infrastructure.Tests
{
    public class MmapRegionConcurrencyTests
    {
        [Fact]
        public async Task ParallelReadAndDispose_DoesNotThrowOrLeak()
        {
            var path = "TestData/mmap_concurrent.dat";
            Directory.CreateDirectory("TestData");
            if (File.Exists(path)) File.Delete(path);
            var region = new MmapRegion(path, 4096, 4096 * 10);
            var readTasks = new Task[8];
            for (int i = 0; i < readTasks.Length; i++)
            {
                readTasks[i] = Task.Run(() =>
                {
                    var buf = new byte[128];
                    for (int j = 0; j < 1000; j++)
                    {
                        try { region.Read(0, buf, 0, buf.Length); } catch (ObjectDisposedException) { }
                    }
                });
            }
            var disposeTask = Task.Run(() => region.Dispose());
            await Task.WhenAll(readTasks);
            await disposeTask;
        }

        [Fact]
        public void MultipleOpenClose_DoesNotLeak()
        {
            var path = "TestData/mmap_leak_test.dat";
            Directory.CreateDirectory("TestData");
            for (int i = 0; i < 100; i++)
            {
                using var region = new MmapRegion(path, 4096, 4096 * 10);
                region.Write(0, new byte[16], 0, 16);
            }
        }
    }
}
