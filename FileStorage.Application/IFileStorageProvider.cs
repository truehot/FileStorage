using FileStorage.Abstractions;

namespace FileStorage
{
    public interface IFileStorageProvider
    {
        ValueTask DisposeAsync();
        Task<IDatabase> GetAsync(CancellationToken ct = default);
    }
}