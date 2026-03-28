using System;
using System.IO;
using FileStorage.Infrastructure.Core.IO;
using Xunit;

namespace FileStorage.Infrastructure.Tests
{
    public class RegionProviderConcurrencyTests
    {
        [Fact]
        public void Reopen_DisposeOldRegion_NoUseAfterDispose()
        {
            var path = "TestData/region_reopen.dat";
            Directory.CreateDirectory("TestData");
            var region1 = new MmapRegion(path, 4096, 4096 * 10);
            var region2 = new MmapRegion(path, 4096, 4096 * 10);
            var provider = new RegionProvider(region1, region2);
            var old = provider.IndexRegion;
            var newRegion = provider.Reopen(old);
            old.Dispose();
            newRegion.Write(0, new byte[8], 0, 8);
            // Should not throw or access disposed region
        }
    }
}
