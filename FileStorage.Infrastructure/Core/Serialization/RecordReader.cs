using FileStorage.Abstractions;
using FileStorage.Infrastructure.Core.IO;

namespace FileStorage.Infrastructure.Core.Serialization;

/// <summary>
/// Reads a StorageRecord from index + data regions by index offset.
/// </summary>
internal sealed class RecordReader : IRecordReader
{
    public StorageRecord? Read(
        IMmapRegion indexRegion,
        IMmapRegion dataRegion,
        byte[] indexBuffer,
        long indexOffset,
        string table,
        Guid key)
    {
        indexRegion.Read(indexOffset, indexBuffer, 0, IndexEntrySerializer.EntryFixedSize);
        var span = indexBuffer.AsSpan(0, IndexEntrySerializer.EntryFixedSize);

        if (IndexEntrySerializer.IsDeleted(span)) return null;

        int dataSize = IndexEntrySerializer.ReadDataSize(span);
        long dataOffset = IndexEntrySerializer.ReadDataOffset(span);
        long version = IndexEntrySerializer.ReadVersion(span);

        var recordData = new byte[dataSize];
        dataRegion.Read(dataOffset, recordData, 0, dataSize);

        return new StorageRecord(
            TableName: table,
            Key: key,
            Data: recordData,
            Version: version,
            IsDeleted: false);
    }
}