namespace FileStorage.Abstractions;

/// <summary>
/// Provides access to the file-based database instance.
/// </summary>
public interface IFileStorageProvider : IAsyncDisposable
{
    /// <summary>
    /// Gets the database instance, creating it lazily if needed. Cancellation is best-effort.
    /// </summary>
    Task<IDatabase> GetAsync(CancellationToken cancellationToken = default);
}