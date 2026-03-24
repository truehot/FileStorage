using FileStorage.Abstractions;
using FileStorage.Abstractions.SecondaryIndex;
using FileStorage.Infrastructure;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;

namespace FileStorage.Application;

/// <summary>
/// Table-level handle. Lightweight object — does not own the engine.
/// Content-level filtering (text search, encoding) lives here,
/// not in the storage engine which operates on raw byte[] only.
/// </summary>
internal sealed class Table : ITable
{
    private readonly IStorageEngine _engine;

    public string Name { get; }

    internal Table(string name, IStorageEngine engine)
    {
        ArgumentNullException.ThrowIfNull(name);
        Name = name;
        _engine = engine;
    }

    public async Task SaveAsync(Guid key, string data, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(data))
            throw new ArgumentException("Data cannot be null or empty.", nameof(data));

        byte[] bytes = Encoding.UTF8.GetBytes(data);

        try
        {
            await _engine.SaveAsync(Name, key, bytes, cancellationToken);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("maximum size"))
        {
            throw new InvalidOperationException(
                $"Storage capacity exceeded. Unable to save record (table: '{Name}', key: {key}, data size: {bytes.Length} bytes).", ex);
        }
    }

    public async Task SaveAsync(Guid key, string data, IReadOnlyDictionary<string, string> indexedFields, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(data))
            throw new ArgumentException("Data cannot be null or empty.", nameof(data));
        ArgumentNullException.ThrowIfNull(indexedFields);

        byte[] bytes = Encoding.UTF8.GetBytes(data);

        try
        {
            await _engine.SaveAsync(Name, key, bytes, indexedFields, cancellationToken);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("maximum size"))
        {
            throw new InvalidOperationException(
                $"Storage capacity exceeded. Unable to save record (table: '{Name}', key: {key}, data size: {bytes.Length} bytes).", ex);
        }
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

        // ── Fast path: use secondary index if available ──
        if (!string.IsNullOrEmpty(filterField) && !string.IsNullOrEmpty(filterValue))
        {
            var keys = await _engine.LookupByIndexAsync(Name, filterField, filterValue, cancellationToken);
            if (keys is not null)
            {
                var result = new List<StorageRecord>();
                int skipped = 0;

                foreach (var key in keys)
                {
                    if (result.Count >= take) break;

                    var record = await _engine.GetByKeyAsync(Name, key, cancellationToken);
                    if (record is null) continue;

                    if (skipped < skip)
                    {
                        skipped++;
                        continue;
                    }

                    result.Add(record);
                }

                return result;
            }
        }

        // ── Slow path: full scan with text matching ──
        if (string.IsNullOrEmpty(filterValue))
        {
            return await _engine.GetByTableAsync(Name, skip, take, cancellationToken);
        }

        var all = await _engine.GetByTableAsync(Name, skip: 0, take: int.MaxValue, cancellationToken);
        var filtered = new List<StorageRecord>();
        int skippedSlow = 0;

        foreach (var record in all)
        {
            if (filtered.Count >= take) break;

            string text = Encoding.UTF8.GetString(record.Data);
            if (!text.Contains(filterValue, StringComparison.OrdinalIgnoreCase))
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
        var all = await _engine.GetByTableAsync(Name, skip: 0, take: int.MaxValue, cancellationToken);

        if (string.IsNullOrEmpty(filterValue))
        {
            foreach (var record in all)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return record;
            }
            yield break;
        }

        byte[] filterBytes = Encoding.UTF8.GetBytes(filterValue);

        foreach (var record in all)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (ContainsBytesOrdinalIgnoreCase(record.Data, filterBytes))
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

    // ── Private helpers ──

    private static bool ContainsBytesOrdinalIgnoreCase(ReadOnlySpan<byte> source, ReadOnlySpan<byte> pattern)
    {
        if (pattern.IsEmpty) return true;
        if (source.Length < pattern.Length) return false;

        int end = source.Length - pattern.Length;
        for (int i = 0; i <= end; i++)
        {
            if (EqualsIgnoreCaseAscii(source.Slice(i, pattern.Length), pattern))
                return true;
        }

        return false;
    }

    private static bool EqualsIgnoreCaseAscii(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        for (int i = 0; i < b.Length; i++)
        {
            byte x = a[i], y = b[i];
            if (x == y) continue;
            if ((uint)((x | 0x20) - 'a') <= 'z' - 'a' && (x | 0x20) == (y | 0x20))
                continue;
            return false;
        }

        return true;
    }
}