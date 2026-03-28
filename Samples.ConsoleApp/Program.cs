using FileStorage.Abstractions;
using FileStorage.Application;
using FileStorage.Application.Extensions;
using System.Text.Json;

string filePath = args.Length > 0 ? args[0] : "Data/FileStorage.db";

Console.WriteLine($"Starting FileStorage with path: {filePath}\n");
await using var provider = new FileStorageProvider(new FileStorageProviderOptions
{
    FilePath = filePath,
    FilterComparisonMode = StringComparison.Ordinal
});
var db = await provider.GetAsync();

// ── Table-level: CRUD ────────────────────────────────────────

var users = db.OpenTable("Users");
var logs = db.OpenTable("Logs");

var key1 = Guid.NewGuid();
var key2 = Guid.NewGuid();

Console.WriteLine("=== Save (string) ===");
await users.SaveAsync(key1, "{\"Name\":\"John Doe\",\"Age\":30}");
await users.SaveAsync(key2, "{\"Name\":\"Jane Doe\",\"Age\":25}");

Console.WriteLine("=== Save (byte[]) ===");
await logs.SaveAsync(Guid.NewGuid(), JsonSerializer.SerializeToUtf8Bytes(new { Event = "Login", User = "John" }, JsonSerializerOptions.Web));
await logs.SaveAsync(Guid.NewGuid(), JsonSerializer.SerializeToUtf8Bytes(new { Event = "Logout", User = "Jane" }, JsonSerializerOptions.Web));

Console.WriteLine($"  Users: {await users.CountAsync()}, Logs: {await logs.CountAsync()}");

Console.WriteLine("\n=== Save (generic, string) ===");
var userModel = new { Id = Guid.NewGuid(), Name = "GenericUser", Age = 40 };
await users.SaveAsync(userModel.Id, userModel, u => JsonSerializer.Serialize(u, JsonSerializerOptions.Web));

Console.WriteLine("\n=== Save (generic, byte[]) ===");
var userModel2 = new { Id = Guid.NewGuid(), Name = "GenericUser2", Age = 41 };
await users.SaveAsync(userModel2.Id, userModel2, u => JsonSerializer.SerializeToUtf8Bytes(u, JsonSerializerOptions.Web));

Console.WriteLine("\n=== Save batch (generic, string) ===");
var batchUsers = new[]
{
    new { Id = Guid.NewGuid(), Name = "Alice", Age = 28 },
    new { Id = Guid.NewGuid(), Name = "Bob", Age = 32 },
    new { Id = Guid.NewGuid(), Name = "Carol", Age = 22 }
};
await users.SaveBatchAsync(
    batchUsers,
    keySelector: x => x.Id,
    dataSelector: x => JsonSerializer.Serialize(x, JsonSerializerOptions.Web)
);

Console.WriteLine("\n=== Save batch (generic, byte[]) ===");
await users.SaveBatchAsync(
    batchUsers,
    keySelector: x => x.Id,
    dataSelector: x => JsonSerializer.SerializeToUtf8Bytes(x, JsonSerializerOptions.Web)
);

Console.WriteLine($"  Users after batch: {await users.CountAsync()}");

Console.WriteLine("\n=== Get ===");
var rec = await users.GetAsync(key1);
Console.WriteLine($"  {rec!.Key}: {rec.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Update (upsert) ===");
await users.SaveAsync(key1, JsonSerializer.SerializeToUtf8Bytes(new { Name = "John Doe", Age = 31 }, JsonSerializerOptions.Web));
var updated = await users.GetAsync(key1);
Console.WriteLine($"  Updated: {updated!.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Filter (all users) ===");
foreach (var u in await users.FilterAsync())
    Console.WriteLine($"  {u.Key}: {u.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Filter (search 'Jane') ===");
foreach (var u in await users.FilterAsync(filterValue: "Jane"))
    Console.WriteLine($"  {u.Key}: {u.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Filter (skip 1, take 1) ===");
foreach (var u in await users.FilterAsync(skip: 1, take: 1))
    Console.WriteLine($"  {u.Key}: {u.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Delete ===");
await users.DeleteAsync(key2);
Console.WriteLine($"  Deleted {key2}");
Console.WriteLine($"  Get deleted: {(await users.GetAsync(key2) is null ? "null ✓" : "still exists!")}");
Console.WriteLine($"  Users remaining: {await users.CountAsync()}");

// ── Table-level: Truncate ────────────────────────────────────

Console.WriteLine("\n=== Truncate 'Logs' ===");
Console.WriteLine($"  Logs before: {await logs.CountAsync()}");
Console.WriteLine($"  Truncated {await logs.TruncateAsync()} records");
Console.WriteLine($"  Logs after: {await logs.CountAsync()}");
Console.WriteLine($"  Table still queryable: {(await logs.FilterAsync()).Count} records (empty is correct)");
// Write new data after truncate — table is still usable
await logs.SaveAsync(Guid.NewGuid(), JsonSerializer.SerializeToUtf8Bytes(new { Event = "Startup", User = "System" }, JsonSerializerOptions.Web));
Console.WriteLine($"  Logs after new write: {await logs.CountAsync()}");

Console.WriteLine("\n=== List tables ===");
foreach (var t in await db.ListTablesAsync())
    Console.WriteLine($"  - {t}");

Console.WriteLine("\n=== Table exists ===");
Console.WriteLine($"  'Users': {await db.TableExistsAsync("Users")}");
Console.WriteLine($"  'Orders': {await db.TableExistsAsync("Orders")}");

Console.WriteLine("\n=== Drop table 'Logs' ===");
Console.WriteLine($"  Dropped {await db.DropTableAsync("Logs")} records");
Console.WriteLine($"  'Logs' exists: {await db.TableExistsAsync("Logs")}");

Console.WriteLine("\n=== Compact 'Users' ===");
Console.WriteLine($"  Removed {await db.CompactAsync(["Users"])} dead records");

Console.WriteLine("\n=== Compact all ===");
Console.WriteLine($"  Removed {await db.CompactAsync()} dead records");

// ── Secondary indexes ────────────────────────────────────────

Console.WriteLine("\n=== Create indexes ===");
var products = db.OpenTable("Products");
await products.EnsureIndexAsync("category");
await products.EnsureIndexAsync("brand");

var info = await products.GetTableInfoAsync();
Console.WriteLine($"  Table: {info.TableName}, Indexes: [{string.Join(", ", info.Indexes.Select(i => i.FieldName))}]");

Console.WriteLine("\n=== Save with indexed fields ===");
// Save with indexed fields
var p1 = Guid.NewGuid();
var p2 = Guid.NewGuid();
var p3 = Guid.NewGuid();
var p4 = Guid.NewGuid();

await products.SaveAsync(p1, JsonSerializer.SerializeToUtf8Bytes(new { Name = "Laptop",  Category = "Electronics", Brand = "Acme" }, JsonSerializerOptions.Web),
    new Dictionary<string, string> { ["category"] = "Electronics", ["brand"] = "Acme" });
await products.SaveAsync(p2, JsonSerializer.SerializeToUtf8Bytes(new { Name = "Phone",   Category = "Electronics", Brand = "Globex" }, JsonSerializerOptions.Web),
    new Dictionary<string, string> { ["category"] = "Electronics", ["brand"] = "Globex" });
await products.SaveAsync(p3, JsonSerializer.SerializeToUtf8Bytes(new { Name = "T-Shirt", Category = "Clothing",    Brand = "Acme" }, JsonSerializerOptions.Web),
    new Dictionary<string, string> { ["category"] = "Clothing", ["brand"] = "Acme" });
await products.SaveAsync(p4, JsonSerializer.SerializeToUtf8Bytes(new { Name = "Jeans",   Category = "Clothing",    Brand = "Globex" }, JsonSerializerOptions.Web),
    new Dictionary<string, string> { ["category"] = "Clothing", ["brand"] = "Globex" });

Console.WriteLine($"  Products: {await products.CountAsync()}");

Console.WriteLine("\n=== Filter by index (category = 'Electronics') ===");
foreach (var p in await products.FilterAsync(filterField: "category", filterValue: "Electronics"))
    Console.WriteLine($"  {p.Key}: {p.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Filter by index (brand = 'Acme') ===");
foreach (var p in await products.FilterAsync(filterField: "brand", filterValue: "Acme"))
    Console.WriteLine($"  {p.Key}: {p.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Filter by index with pagination (category = 'Clothing', skip 1, take 1) ===");
foreach (var p in await products.FilterAsync(filterField: "category", filterValue: "Clothing", skip: 1, take: 1))
    Console.WriteLine($"  {p.Key}: {p.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Delete + index consistency ===");
await products.DeleteAsync(p2);
Console.WriteLine($"  Deleted Phone ({p2})");
var electronics = await products.FilterAsync(filterField: "category", filterValue: "Electronics");
Console.WriteLine($"  Electronics after delete: {electronics.Count} (expected 1)");
foreach (var p in electronics)
    Console.WriteLine($"  {p.Key}: {p.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Stream all products ===");
await foreach (var p in products.StreamAsync())
    Console.WriteLine($"  {p.Key}: {p.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Drop index 'brand' ===");
await products.DropIndexAsync("brand");
info = await products.GetTableInfoAsync();
Console.WriteLine($"  Remaining indexes: [{string.Join(", ", info.Indexes.Select(i => i.FieldName))}]");

Console.WriteLine("\n=== Filter 'brand' after drop (falls back to full scan) ===");
foreach (var p in await products.FilterAsync(filterField: "brand", filterValue: "Acme"))
    Console.WriteLine($"  {p.Key}: {p.GetDataAsUtf8String()}");

Console.WriteLine("\n=== Table info ===");
info = await products.GetTableInfoAsync();
Console.WriteLine($"  Table:   {info.TableName}");
Console.WriteLine($"  Records: {info.RecordCount}");
foreach (var idx in info.Indexes)
    Console.WriteLine($"  Index:   {idx.FieldName} (created {new DateTime(idx.CreatedAtUtc, DateTimeKind.Utc):u})");

// ── Final state ──────────────────────────────────────────────

Console.WriteLine("\n=== Final state ===");
foreach (var t in await db.ListTablesAsync())
{
    var table = db.OpenTable(t);
    Console.WriteLine($"  Table '{t}' ({await table.CountAsync()} records):");
    foreach (var r in await table.FilterAsync())
        Console.WriteLine($"    {r.Key}: {r.GetDataAsUtf8String()}");
}

Console.WriteLine("\nDone.");


