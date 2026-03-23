using FileStorage.Abstractions;
using FileStorage.Infrastructure;

namespace FileStorage.Application;

/// <summary>
/// Database-level handle. Owns the storage engine lifecycle.
/// </summary>
internal sealed class Database : IDatabase
{
    private readonly IStorageEngine _engine;
    private readonly ITableFactory _tableFactory;
    private readonly bool _ownsEngine;
    private bool _disposed;

    internal Database(IStorageEngine engine, ITableFactory? tableFactory = null, bool ownsEngine = true)
    {
        _engine = engine;
        _tableFactory = tableFactory ?? new TableFactory();
        _ownsEngine = ownsEngine;
    }

    public ITable OpenTable(string name)
    {
        ThrowIfDisposed();
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        return _tableFactory.Create(name, _engine);
    }

    public async Task<IReadOnlyList<string>> ListTablesAsync()
    {
        ThrowIfDisposed();
        return await _engine.ListTablesAsync();
    }

    public async Task<bool> TableExistsAsync(string name)
    {
        ThrowIfDisposed();
        return await _engine.TableExistsAsync(name);
    }

    public async Task<long> DropTableAsync(string name)
    {
        ThrowIfDisposed();
        return await _engine.DropTableAsync(name);
    }

    public async Task<long> CompactAsync(params string[] tables)
    {
        ThrowIfDisposed();
        return await _engine.CompactAsync(tables);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        if (_ownsEngine) _engine.Dispose();
    }

    public ValueTask DisposeAsync()
    {
        Dispose();
        return ValueTask.CompletedTask;
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}