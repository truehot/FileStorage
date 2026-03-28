namespace FileStorage.Infrastructure.Core.Configuration;

/// <summary>
/// Configuration options for StorageEngine initialization.
/// Immutable after creation to ensure consistency.
/// </summary>
public sealed class StorageEngineOptions
{
    /// <summary>
    /// Path to the database file (without extension).
    /// Required. Extensions (.idx, .dat, .wal) are added automatically.
    /// </summary>
    public required string FilePath { get; init; }

    /// <summary>
    /// Initial size of the index region in bytes.
    /// Default: 4 MB.
    /// </summary>
    public long IndexInitialSizeBytes { get; init; } = 4 * 1024 * 1024;

    /// <summary>
    /// Maximum size of the index region in bytes.
    /// Default: 1 GB.
    /// </summary>
    public long IndexMaxSizeBytes { get; init; } = 1024L * 1024 * 1024;

    /// <summary>
    /// Initial size of the data region in bytes.
    /// Default: 4 MB.
    /// </summary>
    public long DataInitialSizeBytes { get; init; } = 4 * 1024 * 1024;

    /// <summary>
    /// Maximum size of the data region in bytes.
    /// Default: 10 GB.
    /// </summary>
    public long DataMaxSizeBytes { get; init; } = 10L * 1024 * 1024 * 1024;

    /// <summary>
    /// Number of writes before automatic checkpoint.
    /// Default: 1000.
    /// </summary>
    public int CheckpointWriteThreshold { get; init; } = 1000;

    /// <summary>
    /// Number of memtable entries before flush to SSTable.
    /// Default: 4096.
    /// </summary>
    public int SecondaryIndexFlushThreshold { get; init; } = 4096;

    /// <summary>
    /// Number of SSTables before compaction triggers.
    /// Default: 4.
    /// </summary>
    public int SecondaryIndexCompactionThreshold { get; init; } = 4;

    /// <summary>
    /// Degree of parallelism for bulk reads.
    /// Default: 4.
    /// </summary>
    public int ReadParallelism { get; init; } = 4;

    /// <summary>
    /// String comparison mode for filtering operations.
    /// Supported values: OrdinalIgnoreCase (default) and Ordinal.
    /// </summary>
    public StringComparison FilterComparisonMode { get; init; } = StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// If true, deletes all existing database files (.idx, .dat, .wal, .bloom) before initialization.
    /// Useful for tests and development scenarios to start with a clean state.
    /// WARNING: This is destructive. Use only in test environments.
    /// Default: false (disabled for safety).
    /// </summary>
    public bool DeleteFilesOnStartup { get; init; } = false;

    /// <summary>
    /// If true, deletes all existing database files (.idx, .dat, .wal, .bloom) when provider is disposed.
    /// Useful for tests and temporary environments.
    /// WARNING: This is destructive. Use only in test environments.
    /// Default: false (disabled for safety).
    /// </summary>
    public bool DeleteFilesOnDispose { get; init; } = false;

    /// <summary>
    /// Validates options for consistency.
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrWhiteSpace(FilePath))
            throw new ArgumentException("FilePath is required.", nameof(FilePath));

        if (IndexInitialSizeBytes <= 0)
            throw new ArgumentException("IndexInitialSizeBytes must be positive.", nameof(IndexInitialSizeBytes));

        if (DataInitialSizeBytes <= 0)
            throw new ArgumentException("DataInitialSizeBytes must be positive.", nameof(DataInitialSizeBytes));

        if (IndexMaxSizeBytes < IndexInitialSizeBytes)
            throw new ArgumentException("IndexMaxSizeBytes must be >= IndexInitialSizeBytes.", nameof(IndexMaxSizeBytes));

        if (DataMaxSizeBytes < DataInitialSizeBytes)
            throw new ArgumentException("DataMaxSizeBytes must be >= DataInitialSizeBytes.", nameof(DataMaxSizeBytes));

        if (CheckpointWriteThreshold <= 0)
            throw new ArgumentException("CheckpointWriteThreshold must be positive.", nameof(CheckpointWriteThreshold));

        if (SecondaryIndexFlushThreshold <= 0)
            throw new ArgumentException("SecondaryIndexFlushThreshold must be positive.", nameof(SecondaryIndexFlushThreshold));

        if (SecondaryIndexCompactionThreshold <= 0)
            throw new ArgumentException("SecondaryIndexCompactionThreshold must be positive.", nameof(SecondaryIndexCompactionThreshold));

        if (ReadParallelism <= 0)
            throw new ArgumentException("ReadParallelism must be positive.", nameof(ReadParallelism));

        if (FilterComparisonMode is not StringComparison.OrdinalIgnoreCase and not StringComparison.Ordinal)
            throw new ArgumentException(
                "FilterComparisonMode supports only StringComparison.OrdinalIgnoreCase and StringComparison.Ordinal.",
                nameof(FilterComparisonMode));
    }
}
