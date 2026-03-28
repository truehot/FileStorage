using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Core.Serialization;
using System.Buffers;

namespace FileStorage.Infrastructure.Compaction;

/// <summary>
/// Crash-safe compaction using write-to-temp + atomic rename.
//
// Rename order is deterministic: .dat first, then .idx.
// <see cref="Recovery.StorageRecovery.RecoverInterruptedCompaction"/> relies on this
// order to determine which rename completed before a crash.
/// </summary>
internal sealed class CompactionService : ICompactionService
{
    private const int HeaderSize = 4096;

    private readonly record struct LatestEntryState(long IndexOffset, bool IsDeleted);

    public long Compact(
        IMmapRegion indexRegion,
        IMmapRegion dataRegion,
        IMemoryIndex memoryIndex,
        Func<IMmapRegion, IMmapRegion> reopenRegion,
        IReadOnlySet<string>? tables = null)
    {
        long deadCount = CountDeadRecords(indexRegion, tables);

        if (deadCount == 0)
            return 0;

        string idxTmpPath = indexRegion.Path + ".tmp";
        string datTmpPath = dataRegion.Path + ".tmp";

        var latestInScope = BuildLatestEntryState(indexRegion, tables);

        long liveCount;
        try
        {
            liveCount = StreamToTempFiles(idxTmpPath, datTmpPath, indexRegion, dataRegion, tables, latestInScope);
        }
        catch
        {
            TryDeleteFile(idxTmpPath);
            TryDeleteFile(datTmpPath);
            throw;
        }

        string idxPath = indexRegion.Path;
        string datPath = dataRegion.Path;

        indexRegion.Dispose();
        dataRegion.Dispose();

        File.Move(datTmpPath, datPath, overwrite: true);
        File.Move(idxTmpPath, idxPath, overwrite: true);

        var newIndexRegion = reopenRegion(indexRegion);
        reopenRegion(dataRegion);

        RebuildMemoryIndex(newIndexRegion, memoryIndex, liveCount);

        return deadCount;
    }

    private static long CountDeadRecords(IMmapRegion indexRegion, IReadOnlySet<string>? tables)
    {
        long deadCount = 0;
        long scanPos = HeaderSize;

        var buf = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        try
        {
            while (scanPos + IndexEntrySerializer.EntryFixedSize <= indexRegion.FileSize)
            {
                indexRegion.Read(scanPos, buf, 0, IndexEntrySerializer.EntryFixedSize);
                var span = buf.AsSpan(0, IndexEntrySerializer.EntryFixedSize);

                if (IndexEntrySerializer.IsEmpty(span)) break;

                if (IndexEntrySerializer.IsDeleted(span))
                {
                    string table = IndexEntrySerializer.ReadTableName(span);
                    if (tables is null || tables.Contains(table))
                        deadCount++;
                }

                scanPos += IndexEntrySerializer.EntryFixedSize;
            }
        }
        finally { ArrayPool<byte>.Shared.Return(buf, clearArray: true); }

        return deadCount;
    }

    private static Dictionary<(string Table, Guid Key), LatestEntryState> BuildLatestEntryState(
        IMmapRegion indexRegion,
        IReadOnlySet<string>? tables)
    {
        var latest = new Dictionary<(string Table, Guid Key), LatestEntryState>();
        long scanPos = HeaderSize;

        var buf = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        try
        {
            while (scanPos + IndexEntrySerializer.EntryFixedSize <= indexRegion.FileSize)
            {
                indexRegion.Read(scanPos, buf, 0, IndexEntrySerializer.EntryFixedSize);
                var span = buf.AsSpan(0, IndexEntrySerializer.EntryFixedSize);

                if (IndexEntrySerializer.IsEmpty(span)) break;

                string table = IndexEntrySerializer.ReadTableName(span);
                bool inScope = tables is null || tables.Contains(table);
                if (inScope)
                {
                    Guid key = IndexEntrySerializer.ReadKey(span);
                    bool isDeleted = IndexEntrySerializer.IsDeleted(span);
                    latest[(table, key)] = new LatestEntryState(scanPos, isDeleted);
                }

                scanPos += IndexEntrySerializer.EntryFixedSize;
            }
        }
        finally { ArrayPool<byte>.Shared.Return(buf, clearArray: true); }

        return latest;
    }

    private static long StreamToTempFiles(
        string idxTmpPath,
        string datTmpPath,
        IMmapRegion sourceIndexRegion,
        IMmapRegion sourceDataRegion,
        IReadOnlySet<string>? tables,
        IReadOnlyDictionary<(string Table, Guid Key), LatestEntryState> latestInScope)
    {
        using var datStream = new FileStream(datTmpPath, FileMode.Create, FileAccess.Write, FileShare.None, 65536);
        using var idxStream = new FileStream(idxTmpPath, FileMode.Create, FileAccess.Write, FileShare.None, 65536);

        byte[] header = new byte[HeaderSize];
        header[0] = 0x46;
        header[1] = 1;
        idxStream.Write(header, 0, HeaderSize);

        long newDataPos = 0;
        long liveCount = 0;
        long scanPos = HeaderSize;

        var idxBuf = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        var idxWriteBuf = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        var dataCopyBuf = ArrayPool<byte>.Shared.Rent(64 * 1024);
        try
        {
            while (scanPos + IndexEntrySerializer.EntryFixedSize <= sourceIndexRegion.FileSize)
            {
                sourceIndexRegion.Read(scanPos, idxBuf, 0, IndexEntrySerializer.EntryFixedSize);
                var span = idxBuf.AsSpan(0, IndexEntrySerializer.EntryFixedSize);

                if (IndexEntrySerializer.IsEmpty(span)) break;

                bool isDeleted = IndexEntrySerializer.IsDeleted(span);
                string table = IndexEntrySerializer.ReadTableName(span);
                bool inScope = tables is null || tables.Contains(table);
                Guid key = IndexEntrySerializer.ReadKey(span);

                if (inScope)
                {
                    if (!latestInScope.TryGetValue((table, key), out var latest) ||
                        latest.IndexOffset != scanPos ||
                        latest.IsDeleted)
                    {
                        scanPos += IndexEntrySerializer.EntryFixedSize;
                        continue;
                    }

                    isDeleted = false;
                }

                long oldDataOffset = IndexEntrySerializer.ReadDataOffset(span);
                int dataSize = IndexEntrySerializer.ReadDataSize(span);
                long version = IndexEntrySerializer.ReadVersion(span);

                long entryDataOffset;

                if (!isDeleted && dataSize > 0)
                {
                    entryDataOffset = newDataPos;
                    int remaining = dataSize;
                    long srcPos = oldDataOffset;

                    while (remaining > 0)
                    {
                        int chunk = Math.Min(remaining, dataCopyBuf.Length);
                        sourceDataRegion.Read(srcPos, dataCopyBuf, 0, chunk);
                        datStream.Write(dataCopyBuf, 0, chunk);
                        srcPos += chunk;
                        remaining -= chunk;
                    }

                    newDataPos += dataSize;
                }
                else
                {
                    entryDataOffset = 0;
                }

                var writeSpan = idxWriteBuf.AsSpan(0, IndexEntrySerializer.EntryFixedSize);
                IndexEntrySerializer.Write(writeSpan, table, key, entryDataOffset, dataSize, version);

                if (isDeleted)
                    IndexEntrySerializer.MarkDeleted(idxWriteBuf);

                idxStream.Write(idxWriteBuf, 0, IndexEntrySerializer.EntryFixedSize);
                liveCount++;

                scanPos += IndexEntrySerializer.EntryFixedSize;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(idxBuf, clearArray: true);
            ArrayPool<byte>.Shared.Return(idxWriteBuf, clearArray: true);
            ArrayPool<byte>.Shared.Return(dataCopyBuf);
        }

        datStream.Flush(flushToDisk: true);
        idxStream.Flush(flushToDisk: true);

        return liveCount;
    }

    private static void RebuildMemoryIndex(IMmapRegion indexRegion, IMemoryIndex memoryIndex, long expectedCount)
    {
        memoryIndex.Clear();

        long indexPos = HeaderSize;
        long rebuilt = 0;

        var buf = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        try
        {
            while (indexPos + IndexEntrySerializer.EntryFixedSize <= indexRegion.FileSize && rebuilt < expectedCount)
            {
                indexRegion.Read(indexPos, buf, 0, IndexEntrySerializer.EntryFixedSize);
                var span = buf.AsSpan(0, IndexEntrySerializer.EntryFixedSize);

                if (IndexEntrySerializer.IsEmpty(span)) break;

                if (!IndexEntrySerializer.IsDeleted(span))
                {
                    Guid key = IndexEntrySerializer.ReadKey(span);
                    string table = IndexEntrySerializer.ReadTableName(span);
                    memoryIndex.AddOrUpdate(table, key, indexPos);
                }

                indexPos += IndexEntrySerializer.EntryFixedSize;
                rebuilt++;
            }
        }
        finally { ArrayPool<byte>.Shared.Return(buf, clearArray: true); }
    }

    private static void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch { /* best effort cleanup */ }
    }
}