using FileStorage.Abstractions;
using FileStorage.Abstractions.SecondaryIndex;
using FileStorage.Application.Internal.Filtering;
using FileStorage.Infrastructure;
using FileStorage.Infrastructure.Core.Models;
using System.Runtime.CompilerServices;

namespace FileStorage.Application.Internal;

// Table-level handle. Lightweight object — does not own the engine.
// Content-level filtering (text search, encoding) is implemented here,
// not in the storage engine, which operates on raw byte[] only.
internal sealed class Table : ITable
{
    private readonly IStorageEngine _engine;
    private readonly IRecordContentFilter _recordContentFilter;

    public string Name { get; }

    internal Table(string name, IStorageEngine engine, IRecordContentFilter recordContentFilter)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(recordContentFilter);

        Name = name;
        _engine = engine;
        _recordContentFilter = recordContentFilter;
    }

    public async Task SaveAsync(Guid key, byte[] data, IReadOnlyDictionary<string, string>? indexedFields = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(data);
        if (data.Length == 0)
            throw new ArgumentException("Data cannot be empty.", nameof(data));

        if (indexedFields is null)
        {
            await _engine.SaveAsync(Name, key, data, cancellationToken);
        }
        else
        {
            await _engine.SaveAsync(Name, key, data, indexedFields, cancellationToken);
        }
    }

    public async Task SaveAsync(Guid key, string data, IReadOnlyDictionary<string, string>? indexedFields = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(data);
        if (data.Length == 0)
            throw new ArgumentException("Data cannot be empty.", nameof(data));
        var bytes = System.Text.Encoding.UTF8.GetBytes(data);
        await SaveAsync(key, bytes, indexedFields, cancellationToken);
    }

    public async Task SaveAsync<T>(Guid key, T item, Func<T, string> dataSelector, IReadOnlyDictionary<string, string>? indexedFields = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSelector);
        var data = dataSelector(item);
        await SaveAsync(key, data, indexedFields, cancellationToken);
    }

    public async Task SaveAsync<T>(Guid key, T item, Func<T, byte[]> dataSelector, IReadOnlyDictionary<string, string>? indexedFields = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSelector);
        var data = dataSelector(item);
        await SaveAsync(key, data, indexedFields, cancellationToken);
    }

    /// <summary>
    /// Accepts a generic batch, validates input, serializes each item to <see cref="byte[]"/>
    /// before entering the storage engine, and forwards a normalized batch write.
    /// </summary>
    public async Task SaveBatchAsync<T>(
        IReadOnlyCollection<T> items,
        Func<T, Guid> keySelector,
        Func<T, string> dataSelector,
        Func<T, IReadOnlyDictionary<string, string>>? indexedFieldsSelector = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(dataSelector);
        if (items.Count == 0)
            throw new ArgumentException("Batch cannot be empty.", nameof(items));
        var entries = new List<StorageWriteEntry>(items.Count);
        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Guid key = keySelector(item);
            if (key == Guid.Empty)
                throw new ArgumentException("Batch item key cannot be empty.", nameof(items));
            string data = dataSelector(item);
            ArgumentNullException.ThrowIfNull(data);
            if (data.Length == 0)
                throw new ArgumentException("Batch item data cannot be empty.", nameof(items));
            var indexedFields = indexedFieldsSelector?.Invoke(item) ?? new Dictionary<string, string>();
            ArgumentNullException.ThrowIfNull(indexedFields);
            entries.Add(new StorageWriteEntry(key, System.Text.Encoding.UTF8.GetBytes(data), indexedFields));
        }
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            await _engine.SaveBatchAsync(Name, entries, cancellationToken);
        }
        catch (InvalidOperationException ex) when (IsCapacityExceeded(ex))
        {
            throw new InvalidOperationException(
                $"Storage capacity exceeded. Unable to save batch (table: '{Name}', count: {entries.Count}).", ex);
        }
    }

    public async Task SaveBatchAsync<T>(
        IReadOnlyCollection<T> items,
        Func<T, Guid> keySelector,
        Func<T, byte[]> dataSelector,
        Func<T, IReadOnlyDictionary<string, string>>? indexedFieldsSelector = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(dataSelector);
        if (items.Count == 0)
            throw new ArgumentException("Batch cannot be empty.", nameof(items));
        var entries = new List<StorageWriteEntry>(items.Count);
        foreach (var item in items)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Guid key = keySelector(item);
            if (key == Guid.Empty)
                throw new ArgumentException("Batch item key cannot be empty.", nameof(items));
            byte[] data = dataSelector(item);
            ArgumentNullException.ThrowIfNull(data);
            if (data.Length == 0)
                throw new ArgumentException("Batch item data cannot be empty.", nameof(items));
            var indexedFields = indexedFieldsSelector?.Invoke(item) ?? new Dictionary<string, string>();
            ArgumentNullException.ThrowIfNull(indexedFields);
            entries.Add(new StorageWriteEntry(key, data, indexedFields));
        }
        cancellationToken.ThrowIfCancellationRequested();
        await _engine.SaveBatchAsync(Name, entries, cancellationToken);
    }

    public async Task<StorageRecord?> GetAsync(Guid key, CancellationToken cancellationToken = default) =>
        await _engine.GetByKeyAsync(Name, key, cancellationToken);

    public async Task DeleteAsync(Guid key, CancellationToken cancellationToken = default) =>
        await _engine.DeleteAsync(Name, key, cancellationToken);

    public async Task<List<StorageRecord>> FilterAsync(
        string? filterField = null,
        string? filterValue = null,
        int skip = 0,
        int take = int.MaxValue, CancellationToken cancellationToken = default)
    {
        if (skip < 0)
            throw new ArgumentOutOfRangeException(nameof(skip), "Skip must be non-negative.");
        if (take < 0)
            throw new ArgumentOutOfRangeException(nameof(take), "Take must be non-negative.");
        if (take == 0)
            return [];

        bool hasFilterField = !string.IsNullOrEmpty(filterField);
        bool hasFilterValue = !string.IsNullOrEmpty(filterValue);
        if (hasFilterField && !hasFilterValue)
            throw new ArgumentException("filterValue is required when filterField is provided.", nameof(filterValue));

        // ── Fast path: use secondary index if available ──
        if (hasFilterField && hasFilterValue)
        {
            var keys = await _engine.LookupByIndexAsync(Name, filterField!, filterValue!, cancellationToken);
            if (keys is not null)
            {
                return await _engine.GetByKeysAsync(Name, keys, skip, take, cancellationToken);
            }
        }

        // ── No text filter: delegate to engine pagination ──
        if (!hasFilterValue)
        {
            return await _engine.GetByTableAsync(Name, skip, take, cancellationToken);
        }

        // ── Slow path: streaming full-scan with text matching (no full materialization) ──
        var filtered = new List<StorageRecord>();
        int skippedSlow = 0;

        await foreach (var record in _engine.GetByTableStreamAsync(Name, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (filtered.Count >= take) break;

            if (!_recordContentFilter.IsMatch(record.Data, filterValue!))
                continue;

            if (skippedSlow < skip)
            {
                skippedSlow++;
                continue;
            }

            filtered.Add(record);
        }

        return filtered;
    }

    public async IAsyncEnumerable<StorageRecord> StreamAsync(
        string? filterValue = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var record in _engine.GetByTableStreamAsync(Name, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (string.IsNullOrEmpty(filterValue) || _recordContentFilter.IsMatch(record.Data, filterValue))
            {
                yield return record;
            }
        }
    }

    // ── Index management ──

    public async Task<TableInfo> GetTableInfoAsync(CancellationToken cancellationToken = default)
    {
        long count = await _engine.CountAsync(Name, cancellationToken);
        var indexes = await _engine.GetIndexesAsync(Name, cancellationToken);

        return new TableInfo
        {
            TableName = Name,
            RecordCount = count,
            Indexes = indexes
        };
    }

    public async Task DropIndexAsync(string fieldName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(fieldName))
            throw new ArgumentException("Field name cannot be null or empty.", nameof(fieldName));

        await _engine.DropIndexAsync(Name, fieldName, cancellationToken);
    }

    public async Task EnsureIndexAsync(string fieldName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(fieldName))
            throw new ArgumentException("Field name cannot be null or empty.", nameof(fieldName));

        await _engine.EnsureIndexAsync(Name, fieldName, cancellationToken);
    }

    public async Task<long> TruncateAsync(CancellationToken cancellationToken = default) =>
        await _engine.TruncateTableAsync(Name, cancellationToken);

    public async Task<long> CountAsync(CancellationToken cancellationToken = default) =>
        await _engine.CountAsync(Name, cancellationToken);

    private static bool IsCapacityExceeded(InvalidOperationException ex) =>
        ex.Message.Contains("maximum size", StringComparison.OrdinalIgnoreCase);

    public async Task DeleteBatchAsync(IEnumerable<Guid> keys, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(keys);
        await _engine.DeleteBatchAsync(Name, keys, cancellationToken).ConfigureAwait(false);
    }

    public async Task DeleteBatchAsync<T>(IEnumerable<T> items, Func<T, Guid> keySelector, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(items);
        ArgumentNullException.ThrowIfNull(keySelector);
        var keys = items.Select(keySelector);
        await DeleteBatchAsync(keys, cancellationToken).ConfigureAwait(false);
    }
}