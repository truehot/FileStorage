using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.IO;
using FileStorage.Infrastructure.Serialization;
using FileStorage.Infrastructure.WAL;
using System.Buffers;

namespace FileStorage.Infrastructure.Recovery;

/// <summary>
/// Handles cold-start initialization: recovers from interrupted compaction,
/// reads existing index entries, and replays uncommitted WAL entries
/// to restore consistent state.
/// </summary>
internal sealed class StorageRecovery : IStorageRecovery
{
    private const byte Magic0 = 0x46;
    private const int HeaderSize = 4096;

    public record struct RecoveryResult(long IndexWritePos, long DataWritePos);

    /// <summary>
    /// Full initialization sequence:
    /// <list type="number">
    ///   <item>Recover interrupted compaction (.tmp files).</item>
    ///   <item>Load existing index entries into memory.</item>
    ///   <item>Replay WAL for uncommitted changes.</item>
    /// </list>
    /// <para>
    /// <b>WAL / Compaction invariant:</b>
    /// <c>StorageEngine.CompactAsync</c> calls <c>ForceCheckpoint()</c> before compaction,
    /// which flushes all pending writes to disk and truncates the WAL.
    /// Therefore, WAL entries seen during replay always have offsets valid for the
    /// current (possibly compacted) files. If a compaction was interrupted and
    /// recovered in Step 0, the WAL may still contain pre-compaction entries —
    /// these are validated before replay.
    /// </para>
    /// </summary>
    public RecoveryResult Initialize(
        IMmapRegion indexRegion,
        IMmapRegion dataRegion,
        IWriteAheadLog wal,
        IMemoryIndex memoryIndex,
        IIndexManager indexManager)
    {
        // ── Step 0: Recover interrupted compaction ──
        RecoverInterruptedCompaction(indexRegion.Path, dataRegion.Path);

        // ── Step 1: Detect new vs existing database ──
        byte[] header = new byte[4];
        indexRegion.Read(0, header, 0, 4);

        bool isExisting = header[0] == Magic0;

        if (!isExisting)
        {
            header[0] = Magic0;
            header[1] = 1;
            indexRegion.Write(0, header, 0, 4);
        }

        long indexWritePos = HeaderSize;
        long dataWritePos = 0;

        if (isExisting)
            (indexWritePos, dataWritePos) = LoadExistingIndex(indexRegion, memoryIndex);

        // ── Step 2: Replay WAL ──
        ReplayWal(wal, indexRegion, dataRegion, indexManager, indexWritePos, dataWritePos);

        return new RecoveryResult(indexWritePos, dataWritePos);
    }

    /// <summary>
    /// Detects and recovers from an interrupted compaction.
    /// <para>
    /// Compaction writes two temp files (<c>.idx.tmp</c> and <c>.dat.tmp</c>) and then
    /// renames them over the originals. Since two renames cannot be atomic together,
    /// a crash between them leaves the database in one of these states:
    /// </para>
    /// <list type="table">
    ///   <listheader><term>State</term><description>Action</description></listheader>
    ///   <item>
    ///     <term>Both .tmp exist</term>
    ///     <description>Compaction never started renaming. Delete both .tmp — originals are intact.</description>
    ///   </item>
    ///   <item>
    ///     <term>Only .idx.tmp exists (.dat was already renamed)</term>
    ///     <description>Finish the job: rename .idx.tmp → .idx.</description>
    ///   </item>
    ///   <item>
    ///     <term>Only .dat.tmp exists (.idx was already renamed)</term>
    ///     <description>Finish the job: rename .dat.tmp → .dat.</description>
    ///   </item>
    ///   <item>
    ///     <term>No .tmp files</term>
    ///     <description>Nothing to recover — compaction completed or never started.</description>
    ///   </item>
    /// </list>
    /// </summary>
    private static void RecoverInterruptedCompaction(string idxPath, string datPath)
    {
        string idxTmp = idxPath + ".tmp";
        string datTmp = datPath + ".tmp";

        bool idxTmpExists = File.Exists(idxTmp);
        bool datTmpExists = File.Exists(datTmp);

        if (!idxTmpExists && !datTmpExists)
            return;

        if (idxTmpExists && datTmpExists)
        {
            TryDeleteFile(idxTmp);
            TryDeleteFile(datTmp);
            return;
        }

        if (idxTmpExists && !datTmpExists)
        {
            File.Move(idxTmp, idxPath, overwrite: true);
            return;
        }

        if (!idxTmpExists && datTmpExists)
        {
            File.Move(datTmp, datPath, overwrite: true);
        }
    }

    private static (long indexWritePos, long dataWritePos) LoadExistingIndex(
        IMmapRegion indexRegion, IMemoryIndex memoryIndex)
    {
        memoryIndex.Clear();
        long indexWritePos = HeaderSize;
        long dataWritePos = 0;

        var buffer = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        try
        {
            while ((indexWritePos + IndexEntrySerializer.EntryFixedSize) <= indexRegion.FileSize)
            {
                indexRegion.Read(indexWritePos, buffer, 0, IndexEntrySerializer.EntryFixedSize);
                var span = buffer.AsSpan(0, IndexEntrySerializer.EntryFixedSize);

                if (IndexEntrySerializer.IsEmpty(span)) break;

                Guid key = IndexEntrySerializer.ReadKey(span);
                string table = IndexEntrySerializer.ReadTableName(span);

                if (IndexEntrySerializer.IsDeleted(span))
                    memoryIndex.TryRemove(table, key);
                else
                    memoryIndex.AddOrUpdate(table, key, indexWritePos);

                long dataEnd = IndexEntrySerializer.ReadDataOffset(span) + IndexEntrySerializer.ReadDataSize(span);
                if (dataEnd > dataWritePos)
                    dataWritePos = dataEnd;

                indexWritePos += IndexEntrySerializer.EntryFixedSize;
            }
        }
        finally { ArrayPool<byte>.Shared.Return(buffer, clearArray: true); }

        return (indexWritePos, dataWritePos);
    }

    /// <summary>
    /// Replays WAL entries with offset validation.
    /// <para>
    /// Normally the WAL is empty after compaction (ForceCheckpoint truncates it).
    /// However, if a crash happened during compaction recovery (Step 0 above),
    /// the WAL may contain stale entries with offsets pointing beyond the
    /// compacted files. These are detected and skipped.
    /// </para>
    /// </summary>
    private static void ReplayWal(
        IWriteAheadLog wal,
        IMmapRegion indexRegion,
        IMmapRegion dataRegion,
        IIndexManager indexManager,
        long indexWritePos,
        long dataWritePos)
    {
        var entries = wal.ReadAll();
        if (entries.Count == 0) return;

        bool applied = false;

        foreach (var entry in entries)
        {
            switch (entry.Operation)
            {
                case WalOperationType.Save:
                    // Validate offsets are within current file bounds.
                    // After compaction + recovery, stale WAL entries may reference
                    // offsets that no longer exist in the compacted files.
                    if (IsStaleEntry(entry, indexRegion, dataRegion))
                        continue;

                    indexManager.ApplySave(entry.Table, entry.Key, entry.Data, entry.DataOffset, entry.IndexOffset);
                    applied = true;
                    break;

                case WalOperationType.Delete:
                    // Delete entries reference an existing index offset.
                    // If the offset is beyond the file, the record was already
                    // removed by compaction — safe to skip.
                    if (entry.IndexOffset >= indexRegion.FileSize)
                        continue;

                    indexManager.ApplyDelete(entry.Table, entry.Key, entry.IndexOffset);
                    applied = true;
                    break;

                case WalOperationType.DropTable:
                    // DropTable operates on MemoryIndex — always safe to replay.
                    indexManager.ApplyDropTable(entry.Table);
                    applied = true;
                    break;

                case WalOperationType.TruncateTable:
                    indexManager.ApplyTruncateTable(entry.Table);
                    applied = true;
                    break;
            }
        }

        if (applied)
        {
            indexRegion.Flush();
            dataRegion.Flush();
        }

        // Always truncate WAL after replay — even if all entries were stale.
        wal.Checkpoint();
    }

    /// <summary>
    /// Detects WAL entries with offsets that don't fit in the current files.
    /// This happens when the WAL survived a compaction that shrunk the files.
    /// </summary>
    private static bool IsStaleEntry(WalEntry entry, IMmapRegion indexRegion, IMmapRegion dataRegion)
    {
        // Data would be written beyond the current data file.
        if (entry.DataOffset + entry.Data.Length > dataRegion.FileSize)
            return true;

        // Index entry would be written beyond the current index file.
        if (entry.IndexOffset + IndexEntrySerializer.EntryFixedSize > indexRegion.FileSize)
            return true;

        return false;
    }

    private static void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch { /* best effort */ }
    }
}