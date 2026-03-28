using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using FileStorage.Application;
using Xunit;

namespace FileStorage.Application.Tests
{
    public class TableBatchApiTests
    {
        [Fact]
        public async Task SaveBatchAsync_DuplicateKeys_OverwritesLast()
        {
            Directory.CreateDirectory("TestData");
            await using var provider = new FileStorageProvider("TestData/batch_duplicates.db");
            var db = await provider.GetAsync();
            var table = db.OpenTable("Users");
            var id = Guid.NewGuid();
            var users = new[]
            {
                new { Id = id, Name = "A" },
                new { Id = id, Name = "B" }
            };
            await table.SaveBatchAsync(users, u => u.Id, u => JsonSerializer.Serialize(u));
            var rec = await table.GetAsync(id);
            var obj = JsonSerializer.Deserialize<UserModel>(rec!.Data);
            Assert.Equal("B", obj!.Name);
        }

        private class UserModel { public Guid Id { get; set; } public required string Name { get; set; } }

        [Fact]
        public async Task SaveBatchAsync_PartialFailure_Atomicity()
        {
            Directory.CreateDirectory("TestData");
            await using var provider = new FileStorageProvider("TestData/batch_partialfail.db");
            var db = await provider.GetAsync();
            var table = db.OpenTable("Users");
            var users = new[]
            {
                new { Id = Guid.NewGuid(), Name = "A" },
                new { Id = Guid.Empty, Name = "B" }
            };
            await Assert.ThrowsAsync<ArgumentException>(() => table.SaveBatchAsync(users, u => u.Id, u => JsonSerializer.Serialize(u)));
            Assert.Equal(0, await table.CountAsync());
        }

        [Fact]
        public async Task SaveBatchAsync_Cancellation_Throws()
        {
            Directory.CreateDirectory("TestData");
            await using var provider = new FileStorageProvider("TestData/batch_cancel.db");
            var db = await provider.GetAsync();
            var table = db.OpenTable("Users");
            var users = Enumerable.Range(0, 100_000).Select(i => new { Id = Guid.NewGuid(), Name = $"U{i}" }).ToArray();
            using var cts = new CancellationTokenSource();
            static string DataSelector(dynamic u)
            {
                Thread.Sleep(1); // Simulate work and allow cancellation to be observed
                return JsonSerializer.Serialize(u);
            }
            var t = Task.Run(() => table.SaveBatchAsync(users, u => u.Id, DataSelector, cancellationToken: cts.Token));
            await Task.Delay(10);
            cts.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await t);
        }
    }
}
