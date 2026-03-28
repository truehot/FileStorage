using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Core.Serialization;
using System.Buffers;
using System.Text;

namespace FileStorage.Infrastructure.Indexing.Primary;

/// <summary>
/// Manages physical index entries on mmap regions.
/// Owns write position cursors and encapsulates all <see cref="IndexEntrySerializer"/> usage.
/// </summary>
internal sealed class IndexManager : IIndexManager
{
    private readonly IRegionProvider _regions;
    private readonly IMemoryIndex _memoryIndex;

    private long _indexWritePos;
    private long _dataWritePos;

    public int EntrySize => IndexEntrySerializer.EntryFixedSize;
    public long NextDataOffset => _dataWritePos;
    public long NextIndexOffset => _indexWritePos;

    internal IndexManager(IRegionProvider regions, IMemoryIndex memoryIndex)
    {
        ArgumentNullException.ThrowIfNull(regions);
        ArgumentNullException.ThrowIfNull(memoryIndex);
        
        _regions = regions;
        _memoryIndex = memoryIndex;
    }

    /// <summary>
    /// Validates UTF-8 table name length against fixed index entry capacity.
    /// </summary>
    public void ValidateTableName(string table)
    {
        int byteCount = Encoding.UTF8.GetByteCount(table);
        if (byteCount > IndexEntrySerializer.MaxTableNameBytes - 1)
            throw new ArgumentException(
                $"Table name too long ({byteCount} bytes).", nameof(table));
    }

    /// <summary>
    /// Saves a record using current write cursors and returns assigned offsets.
    /// </summary>
    public (long DataOffset, long IndexOffset) ApplySave(string table, Guid key, byte[] data)
    {
        long dataOffset = _dataWritePos;
        long indexOffset = _indexWritePos;

        ApplySave(table, key, data, dataOffset, indexOffset);

        return (dataOffset, indexOffset);
    }

    /// <summary>
    /// Saves a record at explicit offsets and publishes it to the in-memory index.
    /// Used by recovery and by standard write flow.
    /// </summary>
    public void ApplySave(string table, Guid key, byte[] data, long dataOffset, long indexOffset)
    {
        ApplySavePhysical(table, key, data, dataOffset, indexOffset);
        PublishSave(table, key, indexOffset);
    }

    /// <summary>
    /// Writes data and index entry at explicit offsets and advances cursors,
    /// without updating the in-memory index.
    /// </summary>
    public void ApplySavePhysical(string table, Guid key, byte[] data, long dataOffset, long indexOffset)
    {
        var indexRegion = _regions.IndexRegion;
        var dataRegion = _regions.DataRegion;

        dataRegion.EnsureCapacity(dataOffset, data.Length);
        indexRegion.EnsureCapacity(indexOffset, IndexEntrySerializer.EntryFixedSize);

        dataRegion.Write(dataOffset, data, 0, data.Length);

        var buffer = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        try
        {
            long version = dataOffset;
            IndexEntrySerializer.Write(buffer.AsSpan(), table, key, dataOffset, data.Length, version);
            indexRegion.Write(indexOffset, buffer, 0, IndexEntrySerializer.EntryFixedSize);
        }
        finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }

        // Advance cursors
        long dataEnd = dataOffset + data.Length;
        long indexEnd = indexOffset + IndexEntrySerializer.EntryFixedSize;
        if (dataEnd > _dataWritePos) _dataWritePos = dataEnd;
        if (indexEnd > _indexWritePos) _indexWritePos = indexEnd;
    }

    /// <summary>
    /// Publishes an already persisted entry to the in-memory primary index.
    /// </summary>
    public void PublishSave(string table, Guid key, long indexOffset)
    {
        _memoryIndex.AddOrUpdate(table, key, indexOffset);
    }

    /// <summary>
    /// Marks an index entry as deleted on disk and removes it from memory index.
    /// </summary>
    public void ApplyDelete(string table, Guid key, long indexOffset)
    {
        var indexRegion = _regions.IndexRegion;

        var buffer = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        try
        {
            indexRegion.Read(indexOffset, buffer, 0, IndexEntrySerializer.EntryFixedSize);
            var span = buffer.AsSpan(0, IndexEntrySerializer.EntryFixedSize);

            bool matchesEntry = !IndexEntrySerializer.IsEmpty(span)
                                && IndexEntrySerializer.ReadKey(span) == key
                                && IndexEntrySerializer.TableEquals(span, table);

            if (matchesEntry && !IndexEntrySerializer.IsDeleted(span))
            {
                IndexEntrySerializer.MarkDeleted(span);
                indexRegion.Write(indexOffset, buffer, 0, IndexEntrySerializer.EntryFixedSize);
            }

            _memoryIndex.TryRemove(table, key);
        }
        finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }
    }

    /// <summary>
    /// Soft-deletes all entries for a table and removes table keys from memory index.
    /// </summary>
    public void ApplyDropTable(string table)
    {
        MarkTableEntriesDeleted(table);
    }

    /// <summary>
    /// Truncates table content at storage layer by soft-deleting all index entries.
    /// </summary>
    public void ApplyTruncateTable(string table)
    {
        // At the storage level, truncate is identical to drop -
        // soft-delete all index entries and clear MemoryIndex.
        // The semantic difference (table "still exists") is maintained
        // by the engine/application layer, not the index.
        MarkTableEntriesDeleted(table);
    }

    /// <summary>
    /// Re-scans index file to restore write cursors after compaction.
    /// </summary>
    public void RecalculateWritePositions()
    {
        var indexRegion = _regions.IndexRegion;
        const int headerSize = 4096;
        long indexPos = headerSize;
        long maxDataEnd = 0;

        var buffer = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        try
        {
            while (indexPos + IndexEntrySerializer.EntryFixedSize <= indexRegion.FileSize)
            {
                indexRegion.Read(indexPos, buffer, 0, IndexEntrySerializer.EntryFixedSize);
                var span = buffer.AsSpan(0, IndexEntrySerializer.EntryFixedSize);

                if (IndexEntrySerializer.IsEmpty(span)) break;

                long dataEnd = IndexEntrySerializer.ReadDataOffset(span)
                             + IndexEntrySerializer.ReadDataSize(span);
                if (dataEnd > maxDataEnd)
                    maxDataEnd = dataEnd;

                indexPos += IndexEntrySerializer.EntryFixedSize;
            }
        }
        finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }

        _indexWritePos = indexPos;
        _dataWritePos = maxDataEnd;
    }

    /// <summary>
    /// Sets write cursors to recovered values.
    /// </summary>
    public void SetWritePositions(long indexWritePos, long dataWritePos)
    {
        _indexWritePos = indexWritePos;
        _dataWritePos = dataWritePos;
    }

    /// <summary>
    /// Shared implementation: removes table from MemoryIndex and marks
    /// all its index entries as soft-deleted on disk.
    /// </summary>
    private void MarkTableEntriesDeleted(string table)
    {
        var entries = _memoryIndex.RemoveTable(table);
        if (entries.Count == 0) return;

        var indexRegion = _regions.IndexRegion;
        var buffer = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        try
        {
            foreach (var (_, indexOffset) in entries)
            {
                indexRegion.Read(indexOffset, buffer, 0, IndexEntrySerializer.EntryFixedSize);
                if (!IndexEntrySerializer.IsDeleted(buffer))
                {
                    IndexEntrySerializer.MarkDeleted(buffer);
                    indexRegion.Write(indexOffset, buffer, 0, IndexEntrySerializer.EntryFixedSize);
                }
            }
        }
        finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }
    }
}