using FileStorage.Abstractions;
using FileStorage.Application.Validator;
using FileStorage.Infrastructure;

namespace FileStorage.Application.Internal;

/// <summary>
/// Database-level handle. Owns the storage engine lifecycle.
/// </summary>
internal sealed class Database : IDatabase
{
    private readonly IStorageEngine _engine;
    private readonly ITableFactory _tableFactory;
    private readonly bool _ownsEngine;
    private bool _disposed;

    internal Database(IStorageEngine engine, ITableFactory tableFactory, bool ownsEngine = true)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(tableFactory);

        _engine = engine;
        _tableFactory = tableFactory;
        _ownsEngine = ownsEngine;
    }

    public ITable OpenTable(string name)
    {
        ThrowIfDisposed();
        TableValidator.Validate(name);
        return _tableFactory.Create(name, _engine);
    }

    public async Task<IReadOnlyList<string>> ListTablesAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return await _engine.ListTablesAsync(cancellationToken);
    }

    public async Task<bool> TableExistsAsync(string name, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        TableValidator.Validate(name);
        return await _engine.TableExistsAsync(name, cancellationToken);
    }

    public async Task<long> DropTableAsync(string name, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        TableValidator.Validate(name);
        return await _engine.DropTableAsync(name, cancellationToken);
    }

    /// <summary>
    /// Reclaims disk space by rewriting files without soft-deleted records.
    /// Pass table names to compact selectively, or omit to compact all.
    /// Uses atomic file rename - fully crash-safe.
    /// </summary>
    public async Task<long> CompactAsync(string[]? tables, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        string[] tablesToProcess = tables ?? [];

        foreach (var table in tablesToProcess)
        {
            TableValidator.Validate(table);
        }

        return await _engine.CompactAsync(tablesToProcess, cancellationToken);
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore().ConfigureAwait(false);
        Dispose(false); 
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed) return;
        if (disposing && _ownsEngine)
        {
            _engine.Dispose();
        }
        _disposed = true;
    }

    private async ValueTask DisposeAsyncCore()
    {
        if (_disposed) return;
        if (_ownsEngine)
        {
            if (_engine is IAsyncDisposable ad) await ad.DisposeAsync().ConfigureAwait(false);
            else _engine.Dispose();
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}