using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Core.Serialization;
using FileStorage.Infrastructure.WAL;
using System.Buffers;

namespace FileStorage.Infrastructure.Recovery;

/// <summary>
/// Handles cold-start initialization: recovers from interrupted compaction,
/// reads existing index entries, and replays uncommitted WAL entries
/// to restore consistent state.
///
/// <para>
/// <b>BOUNDED CONTEXT: Recovery & Checkpoint</b><br/>
/// This component preserves the following critical invariants (see copilot-instructions.md):
/// <list type="bullet">
///   <item>WAL is the durability boundary: append WAL entry before considering write committed.</item>
///   <item>WAL record integrity is CRC32-verified; replay stops at first invalid/incomplete record.</item>
///   <item>Recovery must be idempotent and safe on partially-written tails.</item>
///   <item>Checkpoint truncates WAL only after index/data state is durable.</item>
/// </list>
/// </para>
/// </summary>
internal sealed class StorageRecovery : IStorageRecovery
{
    private const byte Magic0 = 0x46;
    private const int HeaderSize = 4096;

    public record struct RecoveryResult(long IndexWritePos, long DataWritePos);

    /// <summary>
    /// Full initialization sequence with idempotency guarantees.
    /// <para>
    /// <b>Initialization order (must be preserved):</b>
    /// <list type="number">
    ///   <item>Recover interrupted compaction (.tmp files).</item>
    ///   <item>Load existing index entries into memory.</item>
    ///   <item>Seed write cursors from reconstructed on-disk state.</item>
    ///   <item>Replay WAL with offset validation.</item>
    /// </list>
    /// </para>
    ///
    /// <para>
    /// <b>Idempotency proof:</b>
    /// Recovery must be safe if called multiple times without flushing between invocations.
    /// This is guaranteed by:
    /// <list type="bullet">
    ///   <item>Offset validation: WAL entries applied only if DataOffset and IndexOffset match
    ///     reconstructed write cursors from on-disk state.</item>
    ///   <item>Stale-entry filtering: If offsets do not match, entry is skipped silently.</item>
    ///   <item>Cursor re-seeding: Cursors recalculated from on-disk index before each replay.</item>
    ///   <item>Example: If recovery runs twice without intermediate writes, the second run sees
    ///     the same on-disk state, reconstructs the same cursors, and skips the same entries.</item>
    /// </list>
    /// </para>
    ///
    /// <para>
    /// <b>WAL / Compaction invariant:</b>
    /// <c>StorageEngine.CompactAsync</c> calls <c>ForceCheckpoint()</c> before compaction,
    /// which flushes all pending writes to disk and truncates the WAL.
    /// Therefore, WAL entries seen during replay always have offsets valid for the
    /// current (possibly compacted) files. If a compaction was interrupted and
    /// recovered in Step 0, the WAL may still contain pre-compaction entries —
    /// these are validated and skipped by offset validation.
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
        // Two-phase rename can crash between them. Detect and complete:
        // [both .tmp exist] -> delete both (compaction never started rename)
        // [only .idx.tmp]   -> rename .idx.tmp -> .idx (finish job)
        // [only .dat.tmp]   -> rename .dat.tmp -> .dat (finish job)
        // [no .tmp]         -> nothing to do
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

        // ── Step 2: Load existing index entries and reconstruct write cursors ──
        if (isExisting)
            (indexWritePos, dataWritePos) = LoadExistingIndex(indexRegion, memoryIndex);

        // ── Step 3: Seed write cursors BEFORE replay ──
        // This step is critical for idempotency. Cursors extracted from on-disk state
        // are used as the "expected" values for offset validation during replay.
        // If recovery runs again, the same cursors are re-extracted, and the same
        // WAL entries will be rejected (or re-applied idempotently).
        indexManager.SetWritePositions(indexWritePos, dataWritePos);

        // ── Step 4: Replay WAL with offset validation ──
        // Entries are applied only if their planned offsets match current cursors.
        // Invalid entries (corrupt, or with wrong offsets) are skipped.
        ReplayWal(wal, indexRegion, dataRegion, indexManager);

        // Return effective cursors after replay.
        return new RecoveryResult(indexManager.NextIndexOffset, indexManager.NextDataOffset);
    }

    /// <summary>
    /// Detects and recovers from an interrupted compaction.
    ///
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
    internal static void RecoverInterruptedCompaction(string idxPath, string datPath)
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
    /// Replays WAL entries with cursor-based offset validation for idempotency.
    ///
    /// <para>
    /// <b>Idempotency mechanism:</b><br/>
    /// Each operation type (Save, SaveBatch, Delete, DropTable, TruncateTable) has
    /// specific validation rules. For offset-based operations (Save, SaveBatch, Delete),
    /// the planned offsets MUST match the current write cursors reconstructed from
    /// on-disk state. This ensures:
    /// <list type="bullet">
    ///   <item>Entries from a previous recovery pass are not re-applied (offsets no longer match).</item>
    ///   <item>Gaps or overlaps in offset plans are detected and skipped.</item>
    ///   <item>Corrupted or stale entries do not corrupt the storage state.</item>
    /// </list>
    /// </para>
    ///
    /// <para>
    /// <b>WAL tail handling and safety:</b><br/>
    /// <see cref="IWriteAheadLog.ReadAllStreaming"/> implements single-pass parsing with
    /// CRC32 validation. If any record is corrupted or incomplete, it stops reading and
    /// marks the invalid tail for truncation. The tail is truncated before replay begins,
    /// ensuring only valid entries are processed.
    /// </para>
    ///
    /// <para>
    /// <b>Checkpoint ordering safety:</b><br/>
    /// After replay completes, we flush both index and data regions so primary state is durable.
    /// WAL truncation is intentionally deferred to startup orchestration and occurs only after
    /// secondary-index replay completes successfully.
    /// </para>
    /// </summary>
    private static void ReplayWal(
        IWriteAheadLog wal,
        IMmapRegion indexRegion,
        IMmapRegion dataRegion,
        IIndexManager indexManager)
    {
        bool hasEntries = false;
        bool applied = false;

        // Cursors are the "expected" offsets for the next write.
        // Entries whose planned offsets match these cursors are applied.
        long expectedDataOffset = indexManager.NextDataOffset;
        long expectedIndexOffset = indexManager.NextIndexOffset;

        var deleteValidationBuffer = ArrayPool<byte>.Shared.Rent(IndexEntrySerializer.EntryFixedSize);
        try
        {
            foreach (var entry in wal.ReadAllStreaming())
            {
                hasEntries = true;

                switch (entry.Operation)
                {
                    case WalOperationType.Save:
                        // Validate offset plan before applying.
                        // If offsets don't match, skip (entry from previous recovery or stale state).
                        if (!CanApplySave(entry, expectedDataOffset, expectedIndexOffset))
                            continue;

                        indexManager.ApplySave(entry.Table, entry.Key, entry.Data, entry.DataOffset, entry.IndexOffset);
                        expectedDataOffset += entry.Data.Length;
                        expectedIndexOffset += IndexEntrySerializer.EntryFixedSize;
                        applied = true;
                        break;

                    case WalOperationType.SaveBatch:
                        // Deserialize batch before validating. If deserialization fails, skip.
                        if (!WalBatchPayloadSerializer.TryDeserialize(entry.Data, out var batchEntries))
                            continue;

                        // Validate batch offset plan: all entries must have contiguous, matching offsets.
                        if (!CanApplySaveBatch(entry, batchEntries, expectedDataOffset, expectedIndexOffset))
                            continue;

                        foreach (var batchEntry in batchEntries)
                        {
                            indexManager.ApplySave(entry.Table, batchEntry.Key, batchEntry.Data, batchEntry.DataOffset, batchEntry.IndexOffset);
                            expectedDataOffset += batchEntry.Data.Length;
                            expectedIndexOffset += IndexEntrySerializer.EntryFixedSize;
                        }

                        applied = true;
                        break;

                    case WalOperationType.Delete:
                        // Validate that the index entry still exists at the expected offset
                        // and matches the key/table being deleted.
                        if (!CanApplyDelete(entry, indexRegion, deleteValidationBuffer))
                            continue;

                        indexManager.ApplyDelete(entry.Table, entry.Key, entry.IndexOffset);
                        applied = true;
                        break;

                    case WalOperationType.DeleteBatch:
                        // Batch delete: deserialize keys and delete each from index
                        var keys = WAL.WalBatchDeletePayloadSerializer.Deserialize(entry.Data);
                        foreach (var key in keys)
                        {
                            // No offset validation needed for batch delete (idempotent)
                            indexManager.ApplyDelete(entry.Table, key, 0); // IndexOffset not used for batch
                        }
                        applied = true;
                        break;

                    case WalOperationType.DropTable:
                        // DropTable operates on MemoryIndex — always safe to replay (idempotent).
                        // No offset validation needed.
                        indexManager.ApplyDropTable(entry.Table);
                        applied = true;
                        break;

                    case WalOperationType.TruncateTable:
                        // TruncateTable operates on MemoryIndex — always safe to replay (idempotent).
                        // No offset validation needed.
                        indexManager.ApplyTruncateTable(entry.Table);
                        applied = true;
                        break;
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(deleteValidationBuffer, clearArray: true);
        }

        if (!hasEntries)
            return;

        // Flush both regions to disk to make all applied changes durable.
        // This ensures that if a crash occurs after this point, the next recovery
        // will see the updated on-disk state and reconstruct matching cursors,
        // making the already-applied entries idempotent (skipped on re-run).
        if (applied)
        {
            indexRegion.Flush();
            dataRegion.Flush();
        }

        // WAL truncation is coordinated by startup orchestration after all replay phases
        // (primary + secondary) complete successfully.
    }

    /// <summary>
    /// Validates if a Save operation can be applied based on offset plan.
    /// Save is idempotent only if offsets match current write cursors.
    /// </summary>
    private static bool CanApplySave(WalEntry entry, long expectedDataOffset, long expectedIndexOffset)
    {
        if (string.IsNullOrEmpty(entry.Table))
            return false;

        if (entry.Data.Length <= 0)
            return false;

        // Offset validation: planned offsets must match current cursors.
        // If not, this entry is from a different recovery context and must be skipped.
        if (entry.DataOffset != expectedDataOffset)
            return false;

        if (entry.IndexOffset != expectedIndexOffset)
            return false;

        return true;
    }

    /// <summary>
    /// Validates if a SaveBatch operation can be applied based on contiguous offset plan.
    /// All entries in the batch must have consecutive, matching offsets.
    /// </summary>
    private static bool CanApplySaveBatch(
        WalEntry entry,
        List<WalBatchEntry> batchEntries,
        long expectedDataOffset,
        long expectedIndexOffset)
    {
        if (batchEntries.Count == 0)
            return false;

        // Batch offset plan must start at expected cursors.
        if (entry.DataOffset != expectedDataOffset || entry.IndexOffset != expectedIndexOffset)
            return false;

        // All entries in the batch must have contiguous offsets.
        long dataCursor = expectedDataOffset;
        long indexCursor = expectedIndexOffset;

        foreach (var batchEntry in batchEntries)
        {
            if (batchEntry.Data.Length <= 0)
                return false;

            // Each entry in the batch must occupy consecutive offsets.
            if (batchEntry.DataOffset != dataCursor || batchEntry.IndexOffset != indexCursor)
                return false;

            dataCursor += batchEntry.Data.Length;
            indexCursor += IndexEntrySerializer.EntryFixedSize;
        }

        return true;
    }

    /// <summary>
    /// Validates if a Delete operation can be applied.
    /// Delete is safe only if the index entry still exists and matches key/table.
    /// </summary>
    private static bool CanApplyDelete(WalEntry entry, IMmapRegion indexRegion, byte[] buffer)
    {
        // Offset must be within valid range.
        if (entry.IndexOffset < HeaderSize)
            return false;

        if (entry.IndexOffset + IndexEntrySerializer.EntryFixedSize > indexRegion.FileSize)
            return false;

        // Read the entry at the expected offset.
        indexRegion.Read(entry.IndexOffset, buffer, 0, IndexEntrySerializer.EntryFixedSize);
        var span = buffer.AsSpan(0, IndexEntrySerializer.EntryFixedSize);

        // Entry must not be empty (already deleted or never existed).
        if (IndexEntrySerializer.IsEmpty(span))
            return false;

        // Entry must match the key and table being deleted (not a different entry reusing the offset).
        if (IndexEntrySerializer.ReadKey(span) != entry.Key)
            return false;

        if (!IndexEntrySerializer.TableEquals(span, entry.Table))
            return false;

        return true;
    }

    private static void TryDeleteFile(string path)
    {
        try { if (File.Exists(path)) File.Delete(path); }
        catch { /* best effort */ }
    }
}