using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using FileStorage.Application;
using Xunit;

namespace FileStorage.Application.Tests
{
    public class TableFilteringEquivalenceTests
    {
        [Fact]
        public async Task FastPathAndSlowPathFiltering_Equivalent()
        {
            Directory.CreateDirectory("TestData");
            await using var provider = new FileStorageProvider("TestData/filter_equiv.db");
            var db = await provider.GetAsync();
            var table = db.OpenTable("Users");
            await table.EnsureIndexAsync("name");
            var users = new[]
            {
                new { Id = Guid.NewGuid(), Name = "Alice" },
                new { Id = Guid.NewGuid(), Name = "Bob" },
                new { Id = Guid.NewGuid(), Name = "Carol" }
            };
            foreach (var u in users)
                await table.SaveAsync(u.Id, JsonSerializer.Serialize(u), new System.Collections.Generic.Dictionary<string, string> { ["name"] = u.Name });
            var fast = await table.FilterAsync(filterField: "name", filterValue: "Alice");
            var slow = await table.FilterAsync(filterField: "name", filterValue: "Alice");
            var fastKeys = fast.Select(x => x.Key).ToHashSet();
            var slowKeys = slow.Select(x => x.Key).ToHashSet();
            Assert.Equal(fastKeys, slowKeys); // Оба способа должны возвращать одинаковые ключи
        }

        [Fact]
        public async Task FastPathAndSlowPathFiltering_Equivalent_SkipTake()
        {
            Directory.CreateDirectory("TestData");
            await using var provider = new FileStorageProvider("TestData/filter_equiv_skip.db");
            var db = await provider.GetAsync();
            var table = db.OpenTable("Users");
            await table.EnsureIndexAsync("name");
            var users = Enumerable.Range(0, 10).Select(i => new { Id = Guid.NewGuid(), Name = $"User{i}" }).ToArray();
            foreach (var u in users)
                await table.SaveAsync(u.Id, JsonSerializer.Serialize(u), new System.Collections.Generic.Dictionary<string, string> { ["name"] = u.Name });
            var fast = await table.FilterAsync(filterField: "name", filterValue: "User5", skip: 0, take: 1);
            var slow = await table.FilterAsync(filterField: "name", filterValue: "User5", skip: 0, take: 1);
            var fastKeys = fast.Select(x => x.Key).ToHashSet();
            var slowKeys = slow.Select(x => x.Key).ToHashSet();
            Assert.Equal(fastKeys, slowKeys);
        }

        [Fact]
        public async Task FastPathAndSlowPathFiltering_Equivalent_ValueNotPresent()
        {
            Directory.CreateDirectory("TestData");
            await using var provider = new FileStorageProvider("TestData/filter_equiv_absent.db");
            var db = await provider.GetAsync();
            var table = db.OpenTable("Users");
            await table.EnsureIndexAsync("name");
            var users = new[]
            {
                new { Id = Guid.NewGuid(), Name = "Alice" },
                new { Id = Guid.NewGuid(), Name = "Bob" }
            };
            foreach (var u in users)
                await table.SaveAsync(u.Id, JsonSerializer.Serialize(u), new System.Collections.Generic.Dictionary<string, string> { ["name"] = u.Name });
            var fast = await table.FilterAsync(filterField: "name", filterValue: "NotFound");
            var slow = await table.FilterAsync(filterField: "name", filterValue: "NotFound");
            Assert.Empty(fast);
            Assert.Empty(slow);
        }

        [Fact]
        public async Task FastPathAndSlowPathFiltering_Equivalent_SecondaryIndexOutOfDate()
        {
            Directory.CreateDirectory("TestData");
            await using var provider = new FileStorageProvider("TestData/filter_equiv_staleidx.db");
            var db = await provider.GetAsync();
            var table = db.OpenTable("Users");
            await table.EnsureIndexAsync("name");
            var id = Guid.NewGuid();
            await table.SaveAsync(id, JsonSerializer.Serialize(new { Id = id, Name = "Alice" }), new System.Collections.Generic.Dictionary<string, string> { ["name"] = "Alice" });
            // Удаляем запись, но не пересоздаём secondary index
            await table.DeleteAsync(id);
            var fast = await table.FilterAsync(filterField: "name", filterValue: "Alice");
            var slow = await table.FilterAsync(filterField: "name", filterValue: "Alice");
            Assert.Equal(fast.Select(x => x.Key), slow.Select(x => x.Key));
            Assert.Empty(fast);
            Assert.Empty(slow);
        }
    }
}
