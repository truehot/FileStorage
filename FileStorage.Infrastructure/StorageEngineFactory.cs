using FileStorage.Infrastructure.Checkpoint;
using FileStorage.Infrastructure.Compaction;
using FileStorage.Infrastructure.Core.Composition;
using FileStorage.Infrastructure.Core.Configuration;
using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Core.Lifecycle;
using FileStorage.Infrastructure.Core.Operations;
using FileStorage.Infrastructure.Core.Serialization;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Indexing.SecondaryIndex;
using FileStorage.Infrastructure.Recovery;
using FileStorage.Infrastructure.WAL;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FileStorage.Infrastructure;

/// <summary>
/// Factory for creating fully-configured StorageEngine instances.
/// Delegates composition to domain-specific helpers to keep single responsibility.
/// </summary>
internal static class StorageEngineFactory
{
    /// <summary>
    /// Creates an engine from StorageEngineOptions with all default dependencies.
    /// This is the primary entry point for application code.
    /// </summary>
    public static async Task<IStorageEngine> CreateAsync(StorageEngineOptions options, ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();

        logger ??= NullLogger.Instance;

        FileLock? fileLock = null;
        IRegionProvider? regions = null;
        IWriteAheadLog? wal = null;
        ISecondaryIndexManager? secondaryIndex = null;
        StorageEngine? engine = null;

        try
        {
            fileLock = FileLock.Acquire(options.FilePath);

            if (options.DeleteFilesOnStartup)
            {
                CleanupDatabaseFiles(options.FilePath, logger);
            }

            string indexPath = options.FilePath + ".idx";
            string dataPath = options.FilePath + ".dat";

            StorageRecovery.RecoverInterruptedCompaction(indexPath, dataPath);

            var indexRegion = new MmapRegion(
                indexPath,
                initialSize: options.IndexInitialSizeBytes,
                maxSize: options.IndexMaxSizeBytes);

            var dataRegion = new MmapRegion(
                dataPath,
                initialSize: options.DataInitialSizeBytes,
                maxSize: options.DataMaxSizeBytes);

            wal = new WriteAheadLog(options.FilePath + ".wal");
            regions = new RegionProvider(indexRegion, dataRegion);

            string secondaryIndexRootPath = GetSecondaryIndexRootPath(options.FilePath);
            var (memoryIndex, indexManager, builtSecondaryIndex) = IndexComposition.CreateIndexServices(
                regions,
                secondaryIndexRootPath,
                options.SecondaryIndexFlushThreshold,
                options.SecondaryIndexCompactionThreshold);
            secondaryIndex = builtSecondaryIndex;

            var (checkpointHandle, lifetime, startup, readOperations, writeOperations, indexOperations, maintenanceOperations) =
                StorageEngineComposition.CreateEngineServices(
                    regions,
                    wal,
                    memoryIndex,
                    indexManager,
                    secondaryIndex,
                    options.CheckpointWriteThreshold,
                    options.ReadParallelism);

            engine = new StorageEngine(
                regions,
                wal,
                secondaryIndex,
                checkpointHandle,
                lifetime,
                startup,
                readOperations,
                writeOperations,
                indexOperations,
                maintenanceOperations,
                fileLock);

            await engine.InitializeAsync().ConfigureAwait(false);
            return engine;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create storage engine for path '{FilePath}'.", options.FilePath);

            if (engine is not null)
            {
                SafeDispose(engine, logger, nameof(StorageEngine));
            }
            else
            {
                if (secondaryIndex is IDisposable disposableSecondary)
                    SafeDispose(disposableSecondary, logger, nameof(ISecondaryIndexManager));

                SafeDispose(wal, logger, nameof(IWriteAheadLog));
                SafeDispose(regions, logger, nameof(IRegionProvider));
                SafeDispose(fileLock, logger, nameof(FileLock));
            }

            throw;
        }
    }

    /// <summary>
    /// Creates an engine from pre-built components. Used in tests and advanced scenarios.
    /// </summary>
    public static async Task<IStorageEngine> CreateAsync(
        IRegionProvider regions,
        IWriteAheadLog wal,
        IMemoryIndex memoryIndex,
        IIndexManager indexManager,
        IRecordReader recordReader,
        ICheckpointManager checkpoint,
        IStorageRecovery recovery,
        ICompactionService compaction,
        ISecondaryIndexManager secondaryIndex,
        FileLock? fileLock = null,
        ILogger? logger = null)
    {
        ArgumentNullException.ThrowIfNull(regions);
        ArgumentNullException.ThrowIfNull(wal);
        ArgumentNullException.ThrowIfNull(memoryIndex);
        ArgumentNullException.ThrowIfNull(indexManager);
        ArgumentNullException.ThrowIfNull(recordReader);
        ArgumentNullException.ThrowIfNull(checkpoint);
        ArgumentNullException.ThrowIfNull(recovery);
        ArgumentNullException.ThrowIfNull(compaction);
        ArgumentNullException.ThrowIfNull(secondaryIndex);

        logger ??= NullLogger.Instance;

        StorageEngine? engine = null;

        try
        {
            var checkpointHandle = new CheckpointHandle(checkpoint);
            var lifetime = new StorageEngineLifetime();
            var secondaryIndexReplayService = new SecondaryIndexReplayService(wal, secondaryIndex);
            var startup = new StorageStartupOperations(
                regions,
                wal,
                memoryIndex,
                indexManager,
                recovery,
                secondaryIndex,
                secondaryIndexReplayService);
            var readOperations = new StorageReadOperations(regions, memoryIndex, indexManager, recordReader);
            var writeOperations = new StorageWriteOperations(wal, memoryIndex, indexManager, secondaryIndex, checkpointHandle);
            var indexOperations = new StorageIndexOperations(secondaryIndex);
            var maintenanceOperations = new StorageMaintenanceOperations(
                regions,
                memoryIndex,
                indexManager,
                wal,
                compaction,
                checkpointHandle,
                checkpointThreshold: 1000);

            engine = new StorageEngine(
                regions,
                wal,
                secondaryIndex,
                checkpointHandle,
                lifetime,
                startup,
                readOperations,
                writeOperations,
                indexOperations,
                maintenanceOperations,
                fileLock);

            await engine.InitializeAsync().ConfigureAwait(false);
            return engine;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to create storage engine from injected dependencies.");

            if (engine is not null)
            {
                SafeDispose(engine, logger, nameof(StorageEngine));
            }
            else
            {
                if (secondaryIndex is IDisposable disposableSecondary)
                    SafeDispose(disposableSecondary, logger, nameof(ISecondaryIndexManager));

                SafeDispose(wal, logger, nameof(IWriteAheadLog));
                SafeDispose(regions, logger, nameof(IRegionProvider));
                SafeDispose(fileLock, logger, nameof(FileLock));
            }

            throw;
        }
    }

    /// <summary>
    /// Deletes database files (.idx, .dat, .wal, .bloom) for the specified path.
    /// Useful for test cleanup and ensuring a fresh database state.
    /// </summary>
    private static void CleanupDatabaseFiles(string filePath, ILogger logger)
    {
        var extensions = new[] { ".idx", ".dat", ".wal", ".bloom" };

        foreach (var ext in extensions)
        {
            var file = filePath + ext;
            try
            {
                if (File.Exists(file))
                    File.Delete(file);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to delete database file '{FilePath}' during startup cleanup.", file);
            }
        }

        var indexDir = GetSecondaryIndexRootPath(filePath);
        try
        {
            if (Directory.Exists(indexDir))
                Directory.Delete(indexDir, recursive: true);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to delete secondary index directory '{DirectoryPath}' during startup cleanup.", indexDir);
        }
    }

    private static string GetSecondaryIndexRootPath(string filePath)
    {
        string basePath = Path.GetDirectoryName(filePath) ?? ".";
        string databaseName = Path.GetFileName(filePath);
        return Path.Combine(basePath, "indexes", databaseName);
    }

    private static void SafeDispose(IDisposable? disposable, ILogger logger, string resourceName)
    {
        try
        {
            disposable?.Dispose();
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Best-effort dispose failed for '{ResourceName}'.", resourceName);
        }
    }
}