using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using FileStorage.Application;
using Xunit;

namespace FileStorage.Application.Tests.Internal;

public class TableBatchErrorTests
{
    [Fact]
    public async Task BatchSaveAsync_EmptyData_ReportsError()
    {
        await using var provider = new FileStorageProvider("TestData/batch_error_data.db");
        var db = await provider.GetAsync();
        var table = db.OpenTable("Users");
        var id = Guid.NewGuid();
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            await table.SaveAsync(id, "", new Dictionary<string, string> { ["name"] = "Alice" });
        });
        Assert.Contains("Data cannot be empty", ex.Message);
    }
}
