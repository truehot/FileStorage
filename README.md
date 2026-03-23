📦 FileStorage

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> [!CAUTION]
> **NOT FOR PRODUCTION USE**  
> This library is under active development. API signatures, disk formats, and internal behaviors (including WAL and Indexing) are subject to breaking changes. Targeting **.NET 9**.

🏗 Solution Structure
Plaintext

FileStorage.sln 
├── FileStorage.Abstractions/   # Domain contracts & interfaces (IDatabase, ITable, ISecondaryIndex)
├── FileStorage.Application/    # High-level logic, TableManager, and API Orchestration
├── FileStorage.Infrastructure/ # Low-level engine: MmapStorage, WalJournal, BloomFilter, LsmCompactor
├── Samples.ConsoleApp/         # Comprehensive CLI demo: CRUD, Indexes, Compaction
├── Samples.API/                # Minimal API integration & Dependency Injection example
└── Tests/                      # Unit, Integration, and Crash-Resilience tests

⚡ Key Features
Feature	Technical Implementation
Async-Native	Full IAsyncEnumerable support for non-blocking data streaming.
Crash-Resilience	Monotonic Sequence Numbers + WAL journal with mandatory CRC.
Fast Indexing	Persistent LSM-tree based secondary indexes for complex queries.
Probabilistic Lookups	Integrated Bloom Filters to prevent unnecessary disk I/O.
Atomic Compaction	Manifest-based "Shadow Paging" protocol for safe data merging.
Memory Efficiency	High-performance serialization using BinaryPrimitives.
🚀 Quick Start
C#

// Initialize the engine with custom performance thresholds

await using var provider = new FileStorageProvider("Data/FileStorage.db");
var db = await provider.GetAsync();

// Tables are logical partitions within the storage root
var usersTable = db.OpenTable("users");

// Save with secondary index metadata for fast filtering
var userId = Guid.NewGuid();
var metadata = new Dictionary<string, string> { ["status"] = "active" };

await usersTable.SaveAsync(userId, "{\"name\":\"Alice\"}", metadata);

// Efficient Filtering using Secondary Indexes & Bloom Filters
var activeUsers = usersTable.StreamAsync(filterKey: "status", filterValue: "active");

await foreach (var record in activeUsers)
{
    // record.Data is handled efficiently via internal buffer pooling
    Console.WriteLine($"Found active user: {record.Key}");
}

📂 Samples & Evaluation

    Samples.ConsoleApp: A deep dive into core engine capabilities, including manual compaction triggers and index rebuilding.

    Samples.Api: Demonstrates how to register FileStorage in a DI container using .AddFileStorage() and use it within controllers.

🛠 Layered Responsibility

    Abstractions: Pure interfaces. No concrete logic or heavy dependencies.

    Application: Manages the lifecycle of tables and coordinates between indexes and the main storage.

    Infrastructure: The "Heavy Lifting" layer. Direct manipulation of memory-mapped views and file system calls.

🧠 AI-Context & Architecture

FileStorage is an embedded, LSM-inspired storage engine solution. It is optimized for high-throughput writes and efficient lookups using a combination of Memory-Mapped Files (MMF) and Write-Ahead Logging (WAL).
Core Technical Specs for LLMs:

    Storage Pattern: Append-only logging with background Compaction.

    Durability: Physical durability via FlushFileBuffers (Win32) / fsync (POSIX) on WAL paths.

    Data Integrity: Per-record CRC32C validation to detect "Torn Writes" or bit rot.

    Concurrency: Thread-safe via lease-based locking and System.Threading.Channels for background IO.

    Zero-Copy: Extensive use of Span<T>, Memory<T>, and ArrayPool<byte> to minimize GC pressure.

License

Distributed under the MIT License. See LICENSE for more information.