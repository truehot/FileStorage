using FileStorage.Abstractions;
using FileStorage.Application;
using FileStorage.Infrastructure;

namespace FileStorage;

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

public class FileStorageProvider : IAsyncDisposable
{
    private readonly string _filePath;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private volatile IDatabase? _db;
    private bool _isDisposed;

    public FileStorageProvider(string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        _filePath = filePath;
    }

    /// <summary>
    /// Gets (or creates) the singleton database instance asynchronously.
    /// </summary>
    public async Task<IDatabase> GetAsync()
    {
        if (_isDisposed)
            throw new ObjectDisposedException(nameof(FileStorageProvider));

        if (_db != null) return _db;

        await _semaphore.WaitAsync();
        try
        {
            if (_db == null)
            {
                var engine = await StorageEngineFactory.CreateAsync(_filePath);
                _db = new Database(engine, ownsEngine: true);
            }
            return _db;
        }
        finally
        {
            _semaphore.Release();
        }
    }

    /// <summary>
    /// Disposes the database instance and releases resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await _semaphore.WaitAsync();
        try
        {
            if (_isDisposed) return;

            if (_db is IAsyncDisposable asyncDisposable)
                await asyncDisposable.DisposeAsync();
            else
                _db?.Dispose();

            _isDisposed = true;
        }
        finally
        {
            _semaphore.Release();
            _semaphore.Dispose();
        }
    }
}