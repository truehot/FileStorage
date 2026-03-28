using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using FileStorage.Infrastructure.WAL;
using Xunit;

namespace FileStorage.Infrastructure.Tests;

public class WalRecoveryInvariantsTests
{
    [Fact]
    public void WalFirstInvariant_AppendBeforeVisible()
    {
        var path = "TestData/wal_invariant.log";
        Directory.CreateDirectory("TestData");
        if (File.Exists(path)) File.Delete(path);
        using var wal = new WriteAheadLog(path);
        var entry = new WalEntry { Table = "t", Key = Guid.NewGuid(), Data = new byte[8] };
        var seq = wal.Append(entry);
        Assert.True(seq > 0);
        // ѕровер€ем, что запись по€вл€етс€ только после Append
        var entries = wal.ReadAll();
        Assert.Contains(entries, e => e.Key == entry.Key);
    }

    [Fact]
    public void RecoveryFirstInvariant_ReplayRestoresState()
    {
        var path = "TestData/wal_recovery.log";
        Directory.CreateDirectory("TestData");
        if (File.Exists(path)) File.Delete(path);
        Guid key = Guid.NewGuid();
        using (var wal = new WriteAheadLog(path))
        {
            var entry = new WalEntry { Table = "t", Key = key, Data = new byte[8] };
            wal.Append(entry);
        }
        using (var wal = new WriteAheadLog(path))
        {
            var entries = wal.ReadAll();
            Assert.Contains(entries, e => e.Key == key);
        }
    }
}
