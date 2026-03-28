## 📦 FileStorage

> [!CAUTION]
> NOT FOR PRODUCTION USE > This library is under active development. API signatures, disk formats, and internal behaviors (including WAL and Indexing) are subject to breaking changes. Targeting .NET 9.

FileStorage is an embedded, LSM-inspired storage engine optimized for high-throughput writes and low-latency lookups. It leverages Memory-Mapped Files (MMF) and Write-Ahead Logging (WAL) to balance extreme performance with crash resilience.


### 🏗 Solution Structure

```
FileStorage.sln 
├── FileStorage.Abstractions/                   # Public contracts (IDatabase, ITable, StorageRecord, IFileStorageProvider)
├── FileStorage.Application/                    # Provider entry point and application-level orchestration
├── FileStorage.Infrastructure/                 # Storage engine internals: regions, WAL, indexing, recovery, compaction
├── FileStorage.Extensions.DependencyInjection/ # DI registration extensions
├── Samples.ConsoleApp/                         # Comprehensive CLI demo: CRUD, indexes, compaction
└── Samples.API/                                # Minimal API integration and DI example
```


### ⚡ Key Features

| Feature                | Technical Implementation                                                               |
|-----------------------|-----------------------------------------------------------------------------------------|
| Async-Native          | Full IAsyncEnumerable support for non-blocking data streaming.                          |
| Crash-Resilience      | Monotonic Sequence Numbers + WAL journal with mandatory CRC32C.                         |
| Fast Indexing         | Persistent LSM-tree based secondary indexes for complex queries.                        |
| Probabilistic Lookups | Integrated Bloom Filters to prevent unnecessary disk I/O.                               |
| Atomic Compaction     | Manifest-based "Shadow Paging" protocol for safe data merging.                          |
| Memory Efficiency     | Zero-copy serialization using BinaryPrimitives and ArrayPool.                           |


### 🚀 Quick Start

#### 1. Manual Setup (Console/Library)

```csharp
using FileStorage.Abstractions;
using FileStorage.Application;
using FileStorage.Application.Extensions; // Required for GetDataAsUtf8String()

// 1. Initialize the engine via provider
await using IFileStorageProvider provider = new FileStorageProvider("Data/FileStorage.db");
IDatabase db = await provider.GetAsync();

// 2. Open a table (logical partition)
var usersTable = db.OpenTable("users");

// 3. IMPORTANT: Ensure secondary indexes are initialized before use
await usersTable.EnsureIndexAsync("status");

// 4. Save data with metadata for secondary indexing
var userId = Guid.NewGuid();
var metadata = new Dictionary<string, string> { ["status"] = "active" };

// Supports raw bytes, strings, or custom serializers
await usersTable.SaveAsync(userId, "{\"name\":\"Alice\"}", metadata);

// 5. Filtering by secondary index
var activeUsers = await usersTable.FilterAsync(filterField: "status", filterValue: "active");

await foreach (var record in activeUsers)
{
    // Access raw data or use UTF8 string helper
    Console.WriteLine($"Found: {record.Key}, Data: {record.GetDataAsUtf8String()}");
}
```

#### 2. Dependency Injection (ASP.NET Core)

```csharp
// Registration in Program.cs
builder.Services.AddFileStorageProvider("Data/FileStorage.db", options => {
    options.CheckpointWriteThreshold = 1000;
    options.FilterComparisonMode = StringComparison.OrdinalIgnoreCase;
});

// Usage in Service
public class UserService
{
    private readonly IFileStorageProvider _provider;

    public UserService(IFileStorageProvider provider)
    {
        _provider = provider;
    }

    public async Task CreateUser(User user)
    {
        // Get IDatabase instance asynchronously
        var db = await _provider.GetAsync();
        var table = db.OpenTable("users");

        // Ensure index exists for the field we want to filter later
        await table.EnsureIndexAsync("role");

        // Serialize the object to string (or bytes)
        var json = JsonSerializer.Serialize(user);

        await table.SaveAsync(user.Id, json,
            new Dictionary<string, string> { ["role"] = user.Role });
    }
}
```


### 🔍 Text Filtering & Encoding 

FileStorage stores payloads as raw byte[], but table-level filtering interprets them as UTF-8 text.

- Comparison Modes: Configured via FileStorageProviderOptions.FilterComparisonMode.

    StringComparison.OrdinalIgnoreCase (Default) — for case-insensitive search.

    StringComparison.Ordinal — for strict case-sensitive matching.

- Validation: Supported values are restricted to the two above. Unsupported values throw an exception during provider creation.

- Best Practice: Use UTF-8 encoded textual payloads when relying on content filtering. Use record.GetDataAsUtf8String() from FileStorage.Application.Extensions for consumption.


### 🧠 Architecture

FileStorage is designed as an embedded storage library with a layered architecture:

- `FileStorage.Abstractions` exposes only the public contracts.
- `FileStorage.Application` owns the provider and table/database orchestration.
- `FileStorage.Infrastructure` contains the storage engine internals, including WAL, memory-mapped regions, primary index management, secondary indexes, recovery, and compaction.
- `FileStorage.Extensions.DependencyInjection` provides DI registration helpers.

Internally, the storage engine is composed from focused services for startup, reads, writes, secondary-index operations, and maintenance, while `StorageEngineFactory` and `StorageEngineComposition` assemble the required dependencies.


### 🧪 Test Projects

- `FileStorage.Extensions.DependencyInjection.Tests` — covers DI registration, invalid options, and service wiring for `ServiceCollectionExtensions`.
- `FileStorage.Application.Tests` — covers provider, table, and batch API behavior.
- `FileStorage.Infrastructure.Tests` — covers engine, WAL, mmap, and index internals.


### 📦 Storage Engine & Persistence

Append-only logging provides durable write intent before physical application. Compaction rewrites storage files to reclaim space while preserving crash safety.

Memory-mapped regions (`MmapRegion`) provide efficient access to index and data files and support safe region reopening during compaction.

The Write-Ahead Log (WAL) is the durability boundary and is replayed during recovery to restore consistent state after unexpected shutdowns.


### 🛡️ Data Integrity & Recovery

Per-record CRC32C validation detects incomplete or corrupted WAL records.

Recovery is checkpoint-aware and replays WAL entries after restoring primary index state. Secondary indexes are loaded from disk and then updated from WAL-derived mutations as part of startup.


### ⚡ Performance & Concurrency

The implementation uses `Span<T>`, `Memory<T>`, and `ArrayPool<byte>` heavily to reduce allocations and keep hot paths efficient.

Concurrency is coordinated through engine-level read/write locking together with snapshot-safe region access and lifecycle gating during disposal.

Streaming APIs expose records incrementally via `IAsyncEnumerable<T>` without materializing full result sets up front.


### 🛠 Key Components

`FileStorageProvider`: main application entry point and lifecycle owner for the database handle.

`StorageEngineFactory`: creates and wires the storage engine from validated options.

`StorageEngine`: internal orchestration facade over startup, read, write, index, and maintenance operations.

`MmapRegion`: manages memory-mapped file segments with automatic growth and safe reopening during compaction.

`WriteAheadLog`: append-only durability log used by checkpointing and recovery.

`IndexManager` and `MemoryIndex`: coordinate persistent and in-memory primary index state.

`SecondaryIndexManager`: manages LSM-style secondary indexes, including flush, lookup, and compaction behavior.

`CompactionService`: rewrites fragmented data files to reclaim space safely.

`BloomFilter`: probabilistic pre-check used by secondary-index SSTables.


### 🏮 Advanced Usage Patterns

#### Batch Operations

Batching reduces WAL synchronization overhead and significantly increases write throughput.

#### Batch Writes

- Use `SaveBatchAsync<T>` to write multiple records in a single operation.
- Provide `keySelector` and `dataSerializer` so records are converted to `byte[]` before persistence.
- Optionally provide `indexedFieldsSelector` to update secondary indexes during batch writes.

#### Batch Save Example

```csharp
var users = db.OpenTable("users");

var batch = new[]
{
    new { Id = Guid.NewGuid(), Name = "Alice", Age = 28 },
    new { Id = Guid.NewGuid(), Name = "Bob", Age = 31 }
};

await users.SaveBatchAsync(
    batch,
    keySelector: x => x.Id,
    dataSerializer: x => JsonSerializer.SerializeToUtf8Bytes(new { x.Name, x.Age }, JsonSerializerOptions.Web),
    indexedFieldsSelector: x => new Dictionary<string, string> { ["name"] = x.Name });
```

#### Batch Delete Example

```csharp
// Batch delete by keys (atomic, crash-safe)
var idsToDelete = new[] { id1, id2, id3 };
await usersTable.DeleteBatchAsync(idsToDelete);
```


### ⚠️ Limitations

No Multi-operation Transactions: ACID isolation is limited to single-record operations. Does not implement multi-operation snapshot isolation (MVCC).

Single-Node Engine: Designed as an embedded database; not suitable for distributed/clustered environments.

Experimental API: Internal structures and disk formats are subject to change during the active development phase.


### 📂 Samples & Evaluation

Samples.ConsoleApp: A deep dive into core engine capabilities, including manual compaction triggers and index rebuilding.

Samples.API: Demonstrates how to register FileStorage in a DI container using `.AddFileStorageProvider()` and use it in Minimal API endpoints.


### 📜 License

Distributed under the MIT License. See LICENSE for more information.