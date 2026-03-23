using FileStorage.Infrastructure.IO;
using FileStorage.Infrastructure.Serialization;
using System.Buffers;
using System.Text;

namespace FileStorage.Infrastructure.Indexing.Primary;

/// <summary>
/// Manages physical index entries on mmap regions.
/// Owns write position cursors and encapsulates all <see cref="IndexEntrySerializer"/> usage.
/// </summary>
internal sealed class IndexManager : IIndexManager
{
    private readonly RegionProvider _regions;
    private readonly IMemoryIndex _memoryIndex;

    private long _indexWritePos;
    private long _dataWritePos;

    public int EntrySize => IndexEntrySerializer.EntryFixedSize;
    public long NextDataOffset => _dataWritePos;
    public long NextIndexOffset => _indexWritePos;

    internal IndexManager(RegionProvider regions, IMemoryIndex memoryIndex)
    {
        _regions = regions;
        _memoryIndex = memoryIndex;
    }

    public void ValidateTableName(string table)
    {
        int byteCount = Encoding.UTF8.GetByteCount(table);
        if (byteCount > IndexEntrySerializer.MaxTableNameBytes - 1)
            throw new ArgumentException(
                $"Table name too long ({byteCount} bytes).", nameof(table));
    }

    public (long DataOffset, long IndexOffset) ApplySave(string table, Guid key, byte[] data)
    {
        long dataOffset = _dataWritePos;
        long indexOffset = _indexWritePos;

        ApplySave(table, key, data, dataOffset, indexOffset);

        return (dataOffset, indexOffset);
    }

    public void ApplySave(string table, Guid key, byte[] data, long dataOffset, long indexOffset)
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

        _memoryIndex.AddOrUpdate(table, key, indexOffset);

        // Advance cursors
        long dataEnd = dataOffset + data.Length;
        long indexEnd = indexOffset + IndexEntrySerializer.EntryFixedSize;
        if (dataEnd > _dataWritePos) _dataWritePos = dataEnd;
        if (indexEnd > _indexWritePos) _indexWritePos = indexEnd;
    }

    public void ApplyDelete(string table, Guid key, long indexOffset)
    {
        var indexRegion = _regions.IndexRegion;

        var buffer = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        try
        {
            indexRegion.Read(indexOffset, buffer, 0, IndexEntrySerializer.EntryFixedSize);

            if (!IndexEntrySerializer.IsDeleted(buffer))
            {
                IndexEntrySerializer.MarkDeleted(buffer);
                indexRegion.Write(indexOffset, buffer, 0, IndexEntrySerializer.EntryFixedSize);
            }

            _memoryIndex.TryRemove(table, key);
        }
        finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }
    }

    public void ApplyDropTable(string table)
    {
        MarkTableEntriesDeleted(table);
    }

    public void ApplyTruncateTable(string table)
    {
        // At the storage level, truncate is identical to drop -
        // soft-delete all index entries and clear MemoryIndex.
        // The semantic difference (table "still exists") is maintained
        // by the engine/application layer, not the index.
        MarkTableEntriesDeleted(table);
    }

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