using FileStorage.Abstractions;
using FileStorage.Infrastructure.Core.IO;

namespace FileStorage.Infrastructure.Core.Serialization;

/// <summary>
/// Reads storage records from index + data regions.
/// </summary>
internal interface IRecordReader
{
    StorageRecord? Read(IMmapRegion indexRegion, IMmapRegion dataRegion,
        byte[] indexBuffer, long indexOffset, string table, Guid key);
}