using FileStorage.Infrastructure.IO;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure;

/// <summary>
/// Creates and initializes <see cref="StorageEngine"/> instances.
/// </summary>
internal static class StorageEngineFactory
{
    /// <summary>
    /// Creates an engine from a file path. Builds regions, WAL and file lock automatically.
    /// Used by <c>FileStorageDb.OpenAsync</c> and <c>DatabaseFactory</c>.
    /// </summary>
    public static async Task<IStorageEngine> CreateAsync(string filePath)
    {
        var fileLock = FileLock.Acquire(filePath);

        var indexRegion = new MmapRegion(filePath + ".idx", initialSize: 4 * 1024 * 1024, maxSize: 1024L * 1024 * 1024);
        var dataRegion = new MmapRegion(filePath + ".dat", initialSize: 4 * 1024 * 1024, maxSize: 10L * 1024 * 1024 * 1024);
        var wal = new WriteAheadLog(filePath + ".wal");

        var regions = new RegionProvider(indexRegion, dataRegion);
        var engine = new StorageEngine(regions, wal, fileLock: fileLock);
        await engine.InitializeAsync();
        return engine;
    }

    /// <summary>
    /// Creates an engine from pre-built components. Used in tests and advanced scenarios.
    /// </summary>
    public static async Task<IStorageEngine> CreateAsync(
        IMmapRegion indexRegion,
        IMmapRegion dataRegion,
        IWriteAheadLog wal,
        FileLock? fileLock = null)
    {
        var regions = new RegionProvider(indexRegion, dataRegion);
        var engine = new StorageEngine(regions, wal, fileLock: fileLock);
        await engine.InitializeAsync();
        return engine;
    }
}