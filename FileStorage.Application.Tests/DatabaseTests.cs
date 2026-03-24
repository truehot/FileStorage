using System.Text;
using System.Text.Json;

namespace FileStorage.Application.Tests;

public class DatabaseTests
{
    private static string ToJson<T>(T obj) => JsonSerializer.Serialize(obj, JsonSerializerOptions.Web);
    private static string Decode(byte[] data) => Encoding.UTF8.GetString(data);

    public DatabaseTests()
    {
        Directory.CreateDirectory("TestData");
    }

    [Fact]
    public async Task Can_Create_Read_Update_Delete_Record()
    {
        // Arrange
        await using var provider = new FileStorageProvider("TestData/test1.db");
        var db = await provider.GetAsync();
        var table = db.OpenTable("Users");
        var key = Guid.NewGuid();

        // Act
        await table.SaveAsync(key, ToJson(new { Name = "Alice", Age = 28 }));
        var rec = await table.GetAsync(key);
        await table.SaveAsync(key, ToJson(new { Name = "Alice", Age = 29 }));
        var updated = await table.GetAsync(key);
        await table.DeleteAsync(key);
        var deleted = await table.GetAsync(key);

        // Assert
        Assert.NotNull(rec);
        Assert.Contains("Alice", Decode(rec!.Data));
        Assert.Contains("29", Decode(updated!.Data));
        Assert.Null(deleted);
    }

    [Fact]
    public async Task Can_Create_And_Drop_Table()
    {
        // Arrange
        await using var provider = new FileStorageProvider("TestData/test2.db");
        var db = await provider.GetAsync();
        var table = db.OpenTable("Logs");

        // Act
        await table.SaveAsync(Guid.NewGuid(), ToJson(new { Event = "Test" }));
        var tables = await db.ListTablesAsync();
        var count = await db.DropTableAsync("Logs");
        var exists = await db.TableExistsAsync("Logs");

        // Assert
        Assert.Contains("Logs", tables);
        Assert.True(count > 0);
        Assert.False(exists);
    }

    [Fact]
    public async Task Can_Use_Secondary_Index()
    {
        // Arrange
        await using var provider = new FileStorageProvider("TestData/test3.db");
        var db = await provider.GetAsync();
        var table = db.OpenTable("Products");
        await table.EnsureIndexAsync("category");
        var id = Guid.NewGuid();

        // Act
        await table.SaveAsync(id, ToJson(new { Name = "Laptop", Category = "Electronics" }),
            new Dictionary<string, string> { ["category"] = "Electronics" });
        var filtered = await table.FilterAsync(filterField: "category", filterValue: "Electronics");

        // Assert
        Assert.Single(filtered);
        Assert.Contains("Laptop", Decode(filtered[0].Data));
    }

    [Fact]
    public async Task Can_Truncate_And_Compact_Table()
    {
        // Arrange
        await using var provider = new FileStorageProvider("TestData/test4.db");
        var db = await provider.GetAsync();
        var table = db.OpenTable("Temp");
        await table.SaveAsync(Guid.NewGuid(), ToJson(new { Value = 1 }));
        await table.SaveAsync(Guid.NewGuid(), ToJson(new { Value = 2 }));

        // Act
        var before = await table.CountAsync();
        var removed = await table.TruncateAsync();
        var after = await table.CountAsync();
        var compacted = await db.CompactAsync(["Temp"]);

        // Assert
        Assert.Equal(2, before);
        Assert.Equal(2, removed);
        Assert.Equal(0, after);
        Assert.True(compacted >= 0);
    }
}