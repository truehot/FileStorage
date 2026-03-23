using FileStorage.Abstractions;
using System.Text;
using System.Text.Json;
using FileStorage.Extensions.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddFileStorageProvider("data/api.db");
builder.Services.AddOpenApi();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseHttpsRedirection();

//app.MapControllers();

// ── CRUD ─────────────────────────────────────────────────────

app.MapPost("/api/{table}", async (string table, JsonElement body, IDatabase db) =>
{
    var t = db.OpenTable(table);
    var key = Guid.NewGuid();
    await t.SaveAsync(key, body.GetRawText());
    return Results.Created($"/api/{table}/{key}", new { key });
});

app.MapGet("/api/{table}/{key:guid}", async (string table, Guid key, IDatabase db) =>
{
    var t = db.OpenTable(table);
    var record = await t.GetAsync(key);
    return record is null
        ? Results.NotFound()
        : Results.Ok(new { record.Key, Data = Encoding.UTF8.GetString(record.Data), record.Version });
});

app.MapPut("/api/{table}/{key:guid}", async (string table, Guid key, JsonElement body, IDatabase db) =>
{
    var t = db.OpenTable(table);
    await t.SaveAsync(key, body.GetRawText());
    return Results.NoContent();
});

app.MapDelete("/api/{table}/{key:guid}", async (string table, Guid key, IDatabase db) =>
{
    var t = db.OpenTable(table);
    await t.DeleteAsync(key);
    return Results.NoContent();
});

// ── Query ────────────────────────────────────────────────────

app.MapGet("/api/{table}", async (string table, int? skip, int? take, string? search, IDatabase db) =>
{
    var t = db.OpenTable(table);
    var records = await t.FilterAsync(filterValue: search, skip: skip ?? 0, take: take ?? 100);
    return Results.Ok(records.Select(r => new
    {
        r.Key,
        Data = Encoding.UTF8.GetString(r.Data),
        r.Version
    }));
});

app.MapGet("/api/{table}/count", async (string table, IDatabase db) =>
{
    var t = db.OpenTable(table);
    return Results.Ok(new { count = await t.CountAsync() });
});

// ── Table management ─────────────────────────────────────────

app.MapGet("/api/tables", async (IDatabase db) =>
    Results.Ok(await db.ListTablesAsync()));

app.MapGet("/api/tables/{name}/exists", async (string name, IDatabase db) =>
    Results.Ok(new { exists = await db.TableExistsAsync(name) }));

app.MapDelete("/api/tables/{name}", async (string name, IDatabase db) =>
    Results.Ok(new { removed = await db.DropTableAsync(name) }));

// POST /api/{table}/truncate — removes all records, table stays queryable
app.MapPost("/api/{table}/truncate", async (string table, IDatabase db) =>
{
    var t = db.OpenTable(table);
    var removed = await t.TruncateAsync();
    return Results.Ok(new { removed });
});

// ── Maintenance ──────────────────────────────────────────────

app.MapPost("/api/compact", async (IDatabase db) =>
    Results.Ok(new { removed = await db.CompactAsync() }));

app.MapPost("/api/compact/{table}", async (string table, IDatabase db) =>
    Results.Ok(new { removed = await db.CompactAsync(table) }));

app.Run();
