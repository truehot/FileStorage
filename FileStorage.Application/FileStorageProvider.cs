using FileStorage.Abstractions;
using FileStorage.Application.Internal;
using FileStorage.Application.Internal.Filtering;
using FileStorage.Application.Validator;
using FileStorage.Infrastructure;
using FileStorage.Infrastructure.Core.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FileStorage.Application;

/// <summary>
/// Main entry point for the FileStorage storage engine.
/// </summary>
/// <example>
/// <code>
/// await using var provider = new FileStorageProvider("data/mydb");
/// var db = await provider.GetAsync();/// 
/// var users = db.OpenTable("users");
/// await users.SaveAsync(Guid.NewGuid(), "{\"name\":\"Alice\"}");
/// </code>
/// </example>

public class FileStorageProvider : IFileStorageProvider, IAsyncDisposable, IDisposable
{
    private enum ProviderState
    {
        Active = 0,
        Disposing = 1,
        Disposed = 2
    }

    // Intentionally not disposed:
    // disposing SemaphoreSlim here can race with in-flight callers that already passed
    // state check and are waiting in Wait/WaitAsync, causing ObjectDisposedException.
    // Provider shutdown relies on state gate + DB disposal; SemaphoreSlim is kept alive
    // until process cleanup for safety under concurrent teardown.
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    private readonly StorageEngineOptions _storageOptions;
    private readonly ILogger<FileStorageProvider> _logger;
    private IDatabase? _db;
    private int _state = (int)ProviderState.Active;

    public FileStorageProvider(string filePath)
        : this(filePath, NullLogger<FileStorageProvider>.Instance)
    {
    }

    public FileStorageProvider(string filePath, ILogger<FileStorageProvider> logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        PathValidator.Validate(filePath);
        ArgumentNullException.ThrowIfNull(logger);

        _logger = logger;
        _storageOptions = new StorageEngineOptions
        {
            FilePath = filePath
        };

        _storageOptions.Validate();
    }

    /// <summary>
    /// Creates a provider with custom FileStorageProviderOptions.
    /// Useful for tests and advanced scenarios.
    /// </summary>
    public FileStorageProvider(FileStorageProviderOptions options)
        : this(options, NullLogger<FileStorageProvider>.Instance)
    {
    }

    public FileStorageProvider(FileStorageProviderOptions options, ILogger<FileStorageProvider> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _logger = logger;
        _storageOptions = options.ToStorageEngineOptions();
    }

    /// <summary>
    /// Gets the database instance, creating it lazily if necessary.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The database instance.</returns>
    /// <exception cref="ObjectDisposedException">Thrown if the provider has been disposed.</exception>
    public async Task<IDatabase> GetAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _state) != (int)ProviderState.Active,
            typeof(FileStorageProvider));

        var instance = Volatile.Read(ref _db);
        if (instance != null)
        {
            return instance;
        }

        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ObjectDisposedException.ThrowIf(
                Volatile.Read(ref _state) != (int)ProviderState.Active,
                typeof(FileStorageProvider));

            instance = Volatile.Read(ref _db);
            if (instance == null)
            {
                var engine = await StorageEngineFactory
                    .CreateAsync(_storageOptions, _logger)
                    .ConfigureAwait(false);

                var recordContentFilter = new Utf8RecordContentFilter(_storageOptions.FilterComparisonMode);
                var tableFactory = new TableFactory(recordContentFilter);
                var database = new Database(engine, tableFactory, ownsEngine: true);

                Volatile.Write(ref _db, database);
                instance = database;
            }

            return instance;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// Performs asynchronous cleanup of resources used by the provider.
    /// </summary>
    /// <returns>A task that represents the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(
                ref _state,
                (int)ProviderState.Disposing,
                (int)ProviderState.Active) != (int)ProviderState.Active)
        {
            return;
        }

        await _semaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            var database = Interlocked.Exchange(ref _db, null);
            if (database is IAsyncDisposable asyncDisposable)
            {
                await asyncDisposable.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                database?.Dispose();
            }

            CleanupFilesOnDisposeIfNeeded();
        }
        finally
        {
            Volatile.Write(ref _state, (int)ProviderState.Disposed);
            _semaphore.Release();

            // Note: _semaphore is intentionally not disposed (see field comment above).
            GC.SuppressFinalize(this);
        }
    }

    /// <summary>
    /// Synchronous dispose to support standard IDisposable.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.CompareExchange(
                ref _state,
                (int)ProviderState.Disposing,
                (int)ProviderState.Active) != (int)ProviderState.Active)
        {
            return;
        }

        _semaphore.Wait();
        try
        {
            var database = Interlocked.Exchange(ref _db, null);
            database?.Dispose();

            CleanupFilesOnDisposeIfNeeded();
        }
        finally
        {
            Volatile.Write(ref _state, (int)ProviderState.Disposed);
            _semaphore.Release();

            // Note: _semaphore is intentionally not disposed (see field comment above).
            GC.SuppressFinalize(this);
        }
    }

    private void CleanupFilesOnDisposeIfNeeded()
    {
        if (!_storageOptions.DeleteFilesOnDispose)
        {
            return;
        }

        CleanupDatabaseFiles(_storageOptions.FilePath);
    }

    private void CleanupDatabaseFiles(string filePath)
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
                _logger.LogWarning(ex, "Failed to delete database file '{FilePath}' during cleanup.", file);
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
            _logger.LogWarning(ex, "Failed to delete secondary index directory '{DirectoryPath}' during cleanup.", indexDir);
        }
    }

    private static string GetSecondaryIndexRootPath(string filePath)
    {
        string basePath = Path.GetDirectoryName(filePath) ?? ".";
        string databaseName = Path.GetFileName(filePath);
        return Path.Combine(basePath, "indexes", databaseName);
    }
}