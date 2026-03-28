using System.Text;
using System.Text.Json;

namespace FileStorage.Application.Tests;

public class DatabaseTests
{
    private static string ToJson<T>(T obj) => JsonSerializer.Serialize(obj, JsonSerializerOptions.Web);
    private static byte[] ToBytes<T>(T obj) => JsonSerializer.SerializeToUtf8Bytes(obj, JsonSerializerOptions.Web);
    private static string Decode(byte[] data) => Encoding.UTF8.GetString(data);

    public DatabaseTests()
    {
        Directory.CreateDirectory("TestData");
    }

    private static FileStorageProvider CreateProvider(string filePath, bool deleteFilesOnStartup = true, bool deleteFilesOnDispose = false)
    {
        return new FileStorageProvider(new FileStorageProviderOptions
        {
            FilePath = filePath,
            DeleteFilesOnStartup = deleteFilesOnStartup,
            DeleteFilesOnDispose = deleteFilesOnDispose
        });
    }

    [Fact]
    public async Task Can_Create_Read_Update_Delete_Record()
    {
        // Arrange
        await using var provider = CreateProvider("TestData/test1.db");
        var db = await provider.GetAsync();
        var table = db.OpenTable("Users");
        var key = Guid.NewGuid();

        // Act
        await table.SaveAsync(key, ToBytes(new { Name = "Alice", Age = 28 }));
        var rec = await table.GetAsync(key);
        await table.SaveAsync(key, ToBytes(new { Name = "Alice", Age = 29 }));
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
        await using var provider = CreateProvider("TestData/test2.db");
        var db = await provider.GetAsync();
        var table = db.OpenTable("Logs");

        // Act
        await table.SaveAsync(Guid.NewGuid(), ToBytes(new { Event = "Test" }));
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
        await using var provider = CreateProvider("TestData/test3.db");
        var db = await provider.GetAsync();
        var table = db.OpenTable("Products");
        await table.EnsureIndexAsync("category");
        var id = Guid.NewGuid();

        // Act
        await table.SaveAsync(id, ToBytes(new { Name = "Laptop", Category = "Electronics" }),
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
        await using var provider = CreateProvider("TestData/test4.db");
        var db = await provider.GetAsync();
        var table = db.OpenTable("Temp");
        await table.SaveAsync(Guid.NewGuid(), ToBytes(new { Value = 1 }));
        await table.SaveAsync(Guid.NewGuid(), ToBytes(new { Value = 2 }));

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

    [Fact]
    public async Task Can_Save_Generic_Batch_With_PreSerialized_Bytes()
    {
        await using var provider = CreateProvider("TestData/test5.db");
        var db = await provider.GetAsync();
        var table = db.OpenTable("Users");

        var users = new[]
        {
            new { Id = Guid.NewGuid(), Name = "Alice", Age = 28 },
            new { Id = Guid.NewGuid(), Name = "Bob", Age = 31 },
            new { Id = Guid.NewGuid(), Name = "Carol", Age = 24 }
        };

        // string generic
        await table.SaveBatchAsync(
            users,
            keySelector: u => u.Id,
            dataSelector: u => ToJson(new { u.Name, u.Age }),
            indexedFieldsSelector: u => new Dictionary<string, string> { ["name"] = u.Name });

        // byte[] generic
        await table.SaveBatchAsync(
            users,
            keySelector: u => u.Id,
            dataSelector: u => JsonSerializer.SerializeToUtf8Bytes(new { u.Name, u.Age }, JsonSerializerOptions.Web),
            indexedFieldsSelector: u => new Dictionary<string, string> { ["name"] = u.Name });

        var count = await table.CountAsync();
        var all = await table.FilterAsync();

        Assert.Equal(3, count);
        Assert.Equal(3, all.Count);
    }

    [Fact]
    public async Task DeleteFilesOnStartup_Resets_Previously_Persisted_Data()
    {
        const string path = "TestData/cleanup-reset.db";
        var key = Guid.NewGuid();

        await using (var firstProvider = CreateProvider(path, deleteFilesOnStartup: true))
        {
            var db = await firstProvider.GetAsync();
            var table = db.OpenTable("Users");
            await table.SaveAsync(key, ToBytes(new { Name = "Alice" }));
            Assert.Equal(1, await table.CountAsync());
        }

        await using (var secondProvider = CreateProvider(path, deleteFilesOnStartup: false))
        {
            var db = await secondProvider.GetAsync();
            var table = db.OpenTable("Users");
            Assert.Equal(1, await table.CountAsync());
        }

        await using (var resetProvider = CreateProvider(path, deleteFilesOnStartup: true))
        {
            var db = await resetProvider.GetAsync();
            var table = db.OpenTable("Users");
            Assert.Equal(0, await table.CountAsync());
        }
    }

    [Fact]
    public async Task DeleteFilesOnDispose_Removes_Persisted_Data()
    {
        const string path = "TestData/dispose-cleanup.db";
        var key = Guid.NewGuid();

        await using (var provider = CreateProvider(path, deleteFilesOnStartup: true, deleteFilesOnDispose: false))
        {
            var db = await provider.GetAsync();
            var table = db.OpenTable("Users");
            await table.SaveAsync(key, ToBytes(new { Name = "Alice" }));
            Assert.Equal(1, await table.CountAsync());
        }

        await using (var providerWithCleanup = CreateProvider(path, deleteFilesOnStartup: false, deleteFilesOnDispose: true))
        {
            var db = await providerWithCleanup.GetAsync();
            var table = db.OpenTable("Users");
            Assert.Equal(1, await table.CountAsync());
        }

        await using (var providerAfterDisposeCleanup = CreateProvider(path, deleteFilesOnStartup: false, deleteFilesOnDispose: false))
        {
            var db = await providerAfterDisposeCleanup.GetAsync();
            var table = db.OpenTable("Users");
            Assert.Equal(0, await table.CountAsync());
        }
    }

    [Fact]
    public void ProviderOptions_With_Empty_FilePath_Throws()
    {
        var options = new FileStorageProviderOptions { FilePath = "" };

        Assert.Throws<ArgumentException>(() => new FileStorageProvider(options));
    }

    [Fact]
    public async Task Filter_Uses_Case_Sensitive_Mode_When_Ordinal_Configured()
    {
        const string path = "TestData/filter-case-sensitive.db";

        await using var provider = new FileStorageProvider(new FileStorageProviderOptions
        {
            FilePath = path,
            DeleteFilesOnStartup = true,
            FilterComparisonMode = StringComparison.Ordinal
        });

        var db = await provider.GetAsync();
        var table = db.OpenTable("Users");

        await table.SaveAsync(Guid.NewGuid(), ToBytes(new { Name = "Alice" }));

        var noMatch = await table.FilterAsync(filterValue: "alice");
        var exactMatch = await table.FilterAsync(filterValue: "Alice");

        Assert.Empty(noMatch);
        Assert.Single(exactMatch);
    }

    [Fact]
    public async Task Filter_Uses_Case_Insensitive_Mode_By_Default()
    {
        const string path = "TestData/filter-case-insensitive.db";

        await using var provider = new FileStorageProvider(new FileStorageProviderOptions
        {
            FilePath = path,
            DeleteFilesOnStartup = true
        });

        var db = await provider.GetAsync();
        var table = db.OpenTable("Users");

        await table.SaveAsync(Guid.NewGuid(), ToBytes(new { Name = "Alice" }));

        var match = await table.FilterAsync(filterValue: "alice");

        Assert.Single(match);
    }

    [Fact]
    public void ProviderOptions_With_Unsupported_FilterComparisonMode_Throws_On_Create()
    {
        Assert.Throws<ArgumentException>(() => new FileStorageProvider(new FileStorageProviderOptions
        {
            FilePath = "TestData/invalid-comparison.db",
            DeleteFilesOnStartup = true,
            FilterComparisonMode = StringComparison.CurrentCulture
        }));
    }

    [Fact]
    public async Task Filter_With_Field_Without_Value_Throws_ArgumentException()
    {
        const string path = "TestData/filter-field-without-value.db";

        await using var provider = CreateProvider(path);
        var db = await provider.GetAsync();
        var table = db.OpenTable("Users");

        await table.SaveAsync(Guid.NewGuid(), ToBytes(new { Name = "Alice" }));

        await Assert.ThrowsAsync<ArgumentException>(() => table.FilterAsync(filterField: "name", filterValue: null));
    }

    [Fact]
    public async Task Filter_With_Value_Without_Field_Uses_Content_Filtering()
    {
        const string path = "TestData/filter-value-without-field.db";

        await using var provider = CreateProvider(path);
        var db = await provider.GetAsync();
        var table = db.OpenTable("Users");

        await table.SaveAsync(Guid.NewGuid(), ToBytes(new { Name = "Alice" }));

        var filtered = await table.FilterAsync(filterField: null, filterValue: "Alice");

        Assert.Single(filtered);
    }

    [Fact]
    public async Task Filter_With_Invalid_Utf8_Payload_Does_Not_Match_And_Does_Not_Throw()
    {
        const string path = "TestData/filter-invalid-utf8.db";

        await using var provider = CreateProvider(path);
        var db = await provider.GetAsync();
        var table = db.OpenTable("Users");

        var item = new[]
        {
            new { Id = Guid.NewGuid(), Data = new byte[] { 0xC3, 0x28 } }
        };

        await table.SaveBatchAsync(
            item,
            keySelector: x => x.Id,
            dataSelector: x => x.Data);

        var all = await table.FilterAsync();
        var filtered = await table.FilterAsync(filterValue: "Alice");

        Assert.Single(all);
        Assert.Empty(filtered);
    }

    [Fact]
    public async Task All_SaveAsync_Overloads_Work_Correctly()
    {
        await using var provider = CreateProvider("TestData/test_save_overloads.db", deleteFilesOnStartup: true);
        var db = await provider.GetAsync();
        var table = db.OpenTable("Overloads");

        var keyString = Guid.NewGuid();
        var keyBytes = Guid.NewGuid();
        var keyGenericString = Guid.NewGuid();
        var keyGenericBytes = Guid.NewGuid();

        // Save string
        await table.SaveAsync(keyString, "{\"Name\":\"StringUser\",\"Age\":20}");
        // Save byte[]
        await table.SaveAsync(keyBytes, ToBytes(new { Name = "BytesUser", Age = 21 }));
        // Save generic (string)
        var user1 = new { Name = "GenericStringUser", Age = 22 };
        await table.SaveAsync(keyGenericString, user1, u => ToJson(u));
        // Save generic (byte[])
        var user2 = new { Name = "GenericBytesUser", Age = 23 };
        await table.SaveAsync(keyGenericBytes, user2, u => ToBytes(u));

        // Assert all records are present and correct
        var recString = await table.GetAsync(keyString);
        var recBytes = await table.GetAsync(keyBytes);
        var recGenString = await table.GetAsync(keyGenericString);
        var recGenBytes = await table.GetAsync(keyGenericBytes);

        Assert.NotNull(recString);
        Assert.Contains("StringUser", Decode(recString!.Data));
        Assert.NotNull(recBytes);
        Assert.Contains("BytesUser", Decode(recBytes!.Data));
        Assert.NotNull(recGenString);
        Assert.Contains("GenericStringUser", Decode(recGenString!.Data));
        Assert.NotNull(recGenBytes);
        Assert.Contains("GenericBytesUser", Decode(recGenBytes!.Data));
    }

    [Fact]
    public async Task Can_Batch_Delete_Records()
    {
        await using var provider = CreateProvider("TestData/test_batch_delete.db", deleteFilesOnStartup: true);
        var db = await provider.GetAsync();
        var table = db.OpenTable("BatchDelete");

        var keys = Enumerable.Range(0, 10).Select(_ => Guid.NewGuid()).ToArray();
        foreach (var key in keys)
            await table.SaveAsync(key, ToBytes(new { Name = $"User{key}" }));

        // Ensure all records exist
        foreach (var key in keys)
            Assert.NotNull(await table.GetAsync(key));

        // Batch delete
        await table.DeleteBatchAsync(keys);

        // Ensure all records are deleted
        foreach (var key in keys)
            Assert.Null(await table.GetAsync(key));
    }

    [Fact]
    public async Task Can_Batch_Delete_Records_Generic()
    {
        await using var provider = CreateProvider("TestData/test_batch_delete_generic.db", deleteFilesOnStartup: true);
        var db = await provider.GetAsync();
        var table = db.OpenTable("BatchDeleteGeneric");

        var users = Enumerable.Range(0, 10).Select(i => new { Id = Guid.NewGuid(), Name = $"User{i}" }).ToArray();
        foreach (var user in users)
            await table.SaveAsync(user.Id, ToBytes(user));

        // Ensure all records exist
        foreach (var user in users)
            Assert.NotNull(await table.GetAsync(user.Id));

        // Batch delete (generic)
        await table.DeleteBatchAsync(users, u => u.Id);

        // Ensure all records are deleted
        foreach (var user in users)
            Assert.Null(await table.GetAsync(user.Id));
    }
}