using FileStorage.Abstractions;
using FileStorage.Application.Extensions;
using FileStorage.Extensions.DependencyInjection;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddFileStorageProvider("data/api.db", options =>
{
    options.FilterComparisonMode = StringComparison.OrdinalIgnoreCase;
    options.DeleteFilesOnStartup = false;
});
builder.Services.AddOpenApi();

var app = builder.Build();

if (app.Environment.IsDevelopment())
    app.MapOpenApi();

app.UseHttpsRedirection();

// ── DATABASE-LEVEL ENDPOINTS ─────────────────────────────────────────────

// Returns the list of all tables in the database.
app.MapGet("/api/database/tables", async (IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    return Results.Ok(await db.ListTablesAsync(cancellationToken));
});

// Creates a new table 
app.MapPost("/api/database/tables/{table}", async (string table, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    if (await db.TableExistsAsync(table, cancellationToken))
        return Results.Conflict(new { error = "Table already exists." });
    // Implicit creation: you may create an empty record or just return Ok.
    return Results.Ok(new { created = table });
});

// Deletes a table and all its records.
app.MapDelete("/api/database/tables/{table}", async (string table, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var removed = await db.DropTableAsync(table, cancellationToken);
    return Results.Ok(new { removed });
});

// Compacts the database or selected tables to reclaim disk space.
app.MapPost("/api/database/compact", async (string[]? tables, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var removed = await db.CompactAsync(tables, cancellationToken);
    return Results.Ok(new { removed });
});

// Returns general information about the database.
app.MapGet("/api/database/info", async (IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var tables = await db.ListTablesAsync(cancellationToken);
    return Results.Ok(new { tablesCount = tables.Count, tables });
});

// ── TABLE-LEVEL ENDPOINTS ────────────────────────────────────────────────

// Checks if a table exists.
app.MapGet("/api/database/tables/{table}/exists", async (string table, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    return Results.Ok(new { exists = await db.TableExistsAsync(table, cancellationToken) });
});

// Removes all records from a table but keeps the table itself.
app.MapPost("/api/database/tables/{table}/truncate", async (string table, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var t = db.OpenTable(table);
    var removed = await t.TruncateAsync(cancellationToken);
    return Results.Ok(new { removed });
});

// Returns the number of records in a table.
app.MapGet("/api/database/tables/{table}/count", async (string table, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var t = db.OpenTable(table);
    return Results.Ok(new { count = await t.CountAsync(cancellationToken) });
});

// ── RECORDS CRUD ENDPOINTS ──────────────────────────────────────────────

// Creates a new record in the specified table.
app.MapPost("/api/database/tables/{table}/records", async (string table, JsonElement body, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var t = db.OpenTable(table);
    var key = Guid.NewGuid();
    await t.SaveAsync(key, JsonSerializer.SerializeToUtf8Bytes(body.GetRawText()), cancellationToken: cancellationToken);
    return Results.Created($"/api/database/tables/{table}/records/{key}", new { key });
});

// Returns a record by key from the specified table.
app.MapGet("/api/database/tables/{table}/records/{key:guid}", async (string table, Guid key, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var t = db.OpenTable(table);
    var record = await t.GetAsync(key, cancellationToken);
    return record is null
        ? Results.NotFound()
        : Results.Ok(new { record.Key, Data = record.GetDataAsUtf8String(), record.Version });
});

// Updates a record by key in the specified table.
app.MapPut("/api/database/tables/{table}/records/{key:guid}", async (string table, Guid key, JsonElement body, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var t = db.OpenTable(table);
    await t.SaveAsync(key, JsonSerializer.SerializeToUtf8Bytes(body.GetRawText()), cancellationToken: cancellationToken);
    return Results.NoContent();
});

// Deletes a record by key from the specified table.
app.MapDelete("/api/database/tables/{table}/records/{key:guid}", async (string table, Guid key, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var t = db.OpenTable(table);
    await t.DeleteAsync(key, cancellationToken);
    return Results.NoContent();
});

// Returns a filtered list of records from the specified table.
app.MapGet("/api/database/tables/{table}/records", async (string table, int? skip, int? take, string? search, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var t = db.OpenTable(table);
    var records = await t.FilterAsync(filterValue: search, skip: skip ?? 0, take: take ?? 100, cancellationToken: cancellationToken);
    return Results.Ok(records.Select(r => new
    {
        r.Key,
        Data = r.GetDataAsUtf8String()  
    }));
});

// ── INDEX MANAGEMENT ENDPOINTS ─────────────────────────────────────────

// Creates a secondary index on the specified field.
app.MapPost("/api/database/tables/{table}/indexes", async (string table, string field, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var t = db.OpenTable(table);
    await t.EnsureIndexAsync(field, cancellationToken);
    return Results.Ok();
});

// Deletes a secondary index from the specified field.
app.MapDelete("/api/database/tables/{table}/indexes/{field}", async (string table, string field, IFileStorageProvider provider, CancellationToken cancellationToken) =>
{
    var db = await provider.GetAsync(cancellationToken);
    var t = db.OpenTable(table);
    await t.DropIndexAsync(field, cancellationToken);
    return Results.Ok();
});

app.Run();
