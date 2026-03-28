using FileStorage.Application.Validator;
using FileStorage.Infrastructure.Core.Configuration;

namespace FileStorage.Application;

/// <summary>
/// Configuration options for FileStorageProvider initialization.
/// </summary>
public sealed class FileStorageProviderOptions
{
    /// <summary>
    /// Path to the database file (without extension).
    /// Required. Extensions (.idx, .dat, .wal) are added automatically.
    /// </summary>
    public required string FilePath { get; set; }

    /// <summary>
    /// Number of writes before automatic checkpoint.
    /// Default: 1000.
    /// </summary>
    public int CheckpointWriteThreshold { get; set; } = 1000;

    /// <summary>
    /// Number of memtable entries before flush to SSTable.
    /// Default: 4096.
    /// </summary>
    public int SecondaryIndexFlushThreshold { get; set; } = 4096;

    /// <summary>
    /// Number of SSTables before compaction triggers.
    /// Default: 4.
    /// </summary>
    public int SecondaryIndexCompactionThreshold { get; set; } = 4;

    /// <summary>
    /// Degree of parallelism for bulk reads.
    /// Default: 4.
    /// </summary>
    public int ReadParallelism { get; set; } = 4;

    /// <summary>
    /// String comparison mode for filtering operations.
    /// Supported values: OrdinalIgnoreCase (default) and Ordinal.
    /// </summary>
    public StringComparison FilterComparisonMode { get; set; } = StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// If true, deletes all existing database files (.idx, .dat, .wal, .bloom) before initialization.
    /// Useful for tests and development scenarios to start with a clean state.
    /// WARNING: This is destructive. Use only in test environments.
    /// Default: false (disabled for safety).
    /// </summary>
    public bool DeleteFilesOnStartup { get; set; } = false;

    /// <summary>
    /// If true, deletes all existing database files (.idx, .dat, .wal, .bloom) when provider is disposed.
    /// Useful for tests and temporary environments.
    /// WARNING: This is destructive. Use only in test environments.
    /// Default: false (disabled for safety).
    /// </summary>
    public bool DeleteFilesOnDispose { get; set; } = false;

    internal StorageEngineOptions ToStorageEngineOptions()
    {
        Validate();

        var storageOptions = new StorageEngineOptions
        {
            FilePath = FilePath,
            CheckpointWriteThreshold = CheckpointWriteThreshold,
            SecondaryIndexFlushThreshold = SecondaryIndexFlushThreshold,
            SecondaryIndexCompactionThreshold = SecondaryIndexCompactionThreshold,
            ReadParallelism = ReadParallelism,
            FilterComparisonMode = FilterComparisonMode,
            DeleteFilesOnStartup = DeleteFilesOnStartup,
            DeleteFilesOnDispose = DeleteFilesOnDispose
        };

        storageOptions.Validate();
        return storageOptions;
    }

    internal void Validate()
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(FilePath);
        PathValidator.Validate(FilePath);

        if (FilterComparisonMode is not StringComparison.OrdinalIgnoreCase and not StringComparison.Ordinal)
            throw new ArgumentException(
                "FilterComparisonMode supports only StringComparison.OrdinalIgnoreCase and StringComparison.Ordinal.",
                nameof(FilterComparisonMode));
    }
}
