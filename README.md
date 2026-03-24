📦 FileStorage

    [!CAUTION]
    NOT FOR PRODUCTION USE > This library is under active development. API signatures, disk formats, and internal behaviors (including WAL and Indexing) are subject to breaking changes. Targeting .NET 9.

FileStorage is an embedded, LSM-inspired storage engine optimized for high-throughput writes and low-latency lookups. It leverages Memory-Mapped Files (MMF) and Write-Ahead Logging (WAL) to balance extreme performance with crash resilience.
🏗 Solution Structure
Plaintext
```
FileStorage.sln 
├── FileStorage.Abstractions/   # Domain contracts & interfaces (IDatabase, ITable)
├── FileStorage.Application/    # High-level logic, TableManager, and API Orchestration
├── FileStorage.Infrastructure/ # Low-level engine: MmapStorage, WalJournal, LsmCompactor
├── Samples.ConsoleApp/         # Comprehensive CLI demo: CRUD, Indexes, Compaction
├── Samples.API/                # Minimal API integration & Dependency Injection example
└── Tests/                      # Unit, Integration, and Crash-Resilience tests
```
⚡ Key Features
Feature	Technical Implementation
Async-Native	Full IAsyncEnumerable support for non-blocking data streaming.
Crash-Resilience	Monotonic Sequence Numbers + WAL journal with mandatory CRC32C.
Fast Indexing	Persistent LSM-tree based secondary indexes for complex queries.
Probabilistic Lookups	Integrated Bloom Filters to prevent unnecessary disk I/O.
Atomic Compaction	Manifest-based "Shadow Paging" protocol for safe data merging.
Memory Efficiency	Zero-copy serialization using BinaryPrimitives and ArrayPool.
🚀 Quick Start
```C#

// Initialize the engine via Provider (Dependency Injection ready)
await using var provider = new FileStorageProvider("Data/FileStorage.db");
var db = await provider.GetAsync();

// Tables are logical partitions within the storage root
var usersTable = db.OpenTable("users");

// Save data with secondary index metadata for fast filtering
var userId = Guid.NewGuid();
var metadata = new Dictionary<string, string> { ["status"] = "active" };
await usersTable.SaveAsync(userId, "{\"name\":\"Alice\"}", metadata);

// Efficient Filtering using Secondary Indexes & Bloom Filters
// Streams data directly from memory-mapped regions
var activeUsers = usersTable.StreamAsync(filterKey: "status", filterValue: "active");

await foreach (var record in activeUsers)
{
    // record.Data is handled efficiently via internal buffer pooling
    Console.WriteLine($"Found active user: {record.Key}");
}
```
🧠 Architecture & AI Context

FileStorage is designed for high-concurrency environments where write-amplification must be minimized and read-performance must scale.
📦 Storage Engine & Persistence

    Append-only Logging: High-performance data ingestion with background Compaction and Shadow Paging for safe, atomic file merging.

    Mmap-Powered I/O: Utilizes memory-mapped regions (MmapRegion) for zero-copy access and atomic point-in-time views for concurrent readers at the file level.

    Write-Ahead Log (WAL): Ensures strict durability via FlushFileBuffers (Win32) / fsync (POSIX) to prevent data loss.

🛡️ Data Integrity & Recovery

    Per-record CRC32C: Hardware-accelerated validation to detect "Torn Writes" or bit rot at the storage level.

    Crash Resilience: Automated recovery through WAL replay and checkpointing, ensuring state consistency after unexpected shutdowns.

⚡ Performance & Concurrency

    Zero-Copy Pipeline: Deep integration of Span<T>, Memory<T>, and ArrayPool<byte> to eliminate GC pressure and redundant allocations.

    Non-blocking I/O: Orchestrates background tasks using System.Threading.Channels and lease-based locking for thread-safe coordination.

    Lock-free Reads: Provides consistent file-level access for readers, ensuring they always see a valid state without contention with active writers.

🛠 Key Components

    MmapRegion: Manages memory-mapped file segments with automatic growth and snapshot-based access safety.

    WAL (Write-Ahead Log): The "Source of Truth" for all write operations, used for reconstruction of the index state.

    IndexManager: Coordinates primary and LSM-tree based secondary indexes for optimized lookups.

    CompactionService: Handles background merging of fragmented data files to reclaim space via shadow paging.

    BloomFilter: A probabilistic data structure that speeds up queries by quickly ruling out non-existent keys in secondary indexes.

    FileStorageProvider: The main entry point for Dependency Injection and lifecycle management.

⚠️ Limitations & Roadmap

    No Multi-operation Transactions: ACID isolation is limited to single-record operations. Does not implement multi-operation snapshot isolation (MVCC).

    Single-Node Engine: Designed as an embedded database; not suitable for distributed/clustered environments.

    Experimental API: Internal structures and disk formats are subject to change during the active development phase.

📂 Samples & Evaluation

    Samples.ConsoleApp: A deep dive into core engine capabilities, including manual compaction triggers and index rebuilding.

    Samples.Api: Demonstrates how to register FileStorage in a DI container using .AddFileStorage() and use it within controllers.

📄 License

Distributed under the MIT License. See LICENSE for more information.