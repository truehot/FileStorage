using FileStorage.Abstractions;
using FileStorage.Application.Validator;
using FileStorage.Infrastructure;

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
    private readonly string _filePath;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private IDatabase? _db;
    private bool _isDisposed;
    private bool _isInitialized;

    public FileStorageProvider(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        PathValidator.Validate(filePath);
        _filePath = filePath;
    }

    /// <summary>
    /// Gets the database instance, creating it lazily if necessary.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The database instance.</returns>
    /// <exception cref="ObjectDisposedException">Thrown if the provider has been disposed.</exception>
    public async Task<IDatabase> GetAsync(CancellationToken cancellationToken = default)
    {
        // CA1513 Fix: Use ThrowIf instead of manual if-check
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed), typeof(FileStorageProvider));

        // Fast-path for already initialized instance
        var instance = Volatile.Read(ref _db);
        if (Volatile.Read(ref _isInitialized) && instance != null)
        {
            return instance;
        }

        await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // CA1513 Fix: Double-check after acquiring the lock
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _isDisposed), typeof(FileStorageProvider));

            if (!Volatile.Read(ref _isInitialized))
            {
                // Create the storage engine and database
                var engine = await StorageEngineFactory
                    .CreateAsync(_filePath)
                    .ConfigureAwait(false);

                var database = new Database(engine, ownsEngine: true);

                // Write order matters: first the instance, then the flag
                Volatile.Write(ref _db, database);
                Volatile.Write(ref _isInitialized, true);

                instance = database;
            }
            else
            {
                instance = Volatile.Read(ref _db);
            }

            return instance ?? throw new InvalidOperationException("Database initialization failed");
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
        // 1. Fast-path check without locking
        if (Volatile.Read(ref _isDisposed))
            return;

        await _semaphore.WaitAsync().ConfigureAwait(false);
        try
        {
            // 2. Double-check under lock
            if (Volatile.Read(ref _isDisposed))
                return;

            // Call the internal cleanup method (extensibility point for derived classes)
            await DisposeAsyncCore().ConfigureAwait(false);

            // Mark as fully disposed
            Volatile.Write(ref _isDisposed, true);
        }
        finally
        {
            _semaphore.Release();
        }

        // 3. Notify GC that the finalizer does not need to run
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Core logic for disposing resources. Can be overridden by derived classes.
    /// </summary>
    /// <returns>A task that represents the core dispose operation.</returns>
    protected virtual async ValueTask DisposeAsyncCore()
    {
        Volatile.Write(ref _isInitialized, false);

        var database = Volatile.Read(ref _db);
        if (database != null)
        {
            await database.DisposeAsync().ConfigureAwait(false);
            Volatile.Write(ref _db, null);
        }
    }

    /// <summary>
    /// Synchronous dispose to support standard IDisposable.
    /// Note: This does not block on asynchronous cleanup but ensures GC suppression.
    /// </summary>
    public void Dispose()
    {
        // We cannot safely call asynchronous DisposeAsyncCore here without risking a deadlock.
        // However, we mark the object as disposed and suppress finalization.
        if (Volatile.Read(ref _isDisposed)) return;

        Volatile.Write(ref _isDisposed, true);
        GC.SuppressFinalize(this);
    }
}