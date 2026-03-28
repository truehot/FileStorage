using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure.Checkpoint;

/// <summary>
/// Manages durable checkpoints: tracks write count and flushes regions to disk
/// before truncating the WAL.
///
/// <para>
/// <b>Checkpoint protocol (order is critical for crash-safety):</b><br/>
/// <see cref="ForceCheckpoint"/> executes the following steps in strict order:
/// <list type="number">
///   <item>Flush index region to disk (fsync).</item>
///   <item>Flush data region to disk (fsync).</item>
///   <item>Truncate WAL (SetLength(0) + fsync).</item>
/// </list>
/// </para>
///
/// <para>
/// <b>Why this order is critical:</b><br/>
/// If a crash occurs:
/// <list type="bullet">
///   <item>Before Step 1 or 2: Regions are not flushed. Next recovery finds them in memory,
///     reconstructs cursors from on-disk index, and replays WAL (safe).</item>
///   <item>Between Steps 2 and 3: Both regions are flushed. WAL still exists but is redundant
///     (entries already on disk). Next recovery skips stale entries via offset validation (idempotent).</item>
///   <item>After Step 3: WAL is truncated. No data loss because all writes were flushed (safe).</item>
/// </list>
/// </para>
///
/// <para>
/// <b>Anti-pattern (WRONG order):</b><br/>
/// <c>wal.Checkpoint(); indexRegion.Flush(); dataRegion.Flush();</c> causes data loss
/// because WAL is cleared before regions are flushed. A crash between truncation and flush
/// loses uncommitted writes.
/// </para>
/// </summary>
internal sealed class CheckpointManager(
    IMmapRegion indexRegion,
    IMmapRegion dataRegion,
    IWriteAheadLog wal,
    int threshold = 1000) : ICheckpointManager
{
    private int _writesSinceCheckpoint;

    /// <summary>
    /// Tracks a write operation. Automatically forces a checkpoint when the threshold is reached.
    /// <para>
    /// This method is called after every write (save, delete, batch, etc.) to ensure
    /// periodic durability without explicit checkpoint calls.
    /// </para>
    /// </summary>
    public void TrackWrite()
    {
        _writesSinceCheckpoint++;
        if (_writesSinceCheckpoint >= threshold)
            ForceCheckpoint();
    }

    /// <summary>
    /// Forces an immediate checkpoint regardless of threshold.
    ///
    /// <para>
    /// <b>Safe checkpoint protocol:</b><br/>
    /// Executes the following steps in strict order:
    /// <list type="number">
    ///   <item><see cref="IMmapRegion.Flush"/> on index region (fsync to disk).</item>
    ///   <item><see cref="IMmapRegion.Flush"/> on data region (fsync to disk).</item>
    ///   <item><see cref="IWriteAheadLog.Checkpoint"/> (truncate WAL + fsync).</item>
    /// </list>
    /// </para>
    ///
    /// <para>
    /// <b>Crash-safety guarantee:</b><br/>
    /// After this method returns, all pending writes are durable on disk.
    /// The WAL is empty and ready for new operations. If a crash occurs during
    /// checkpoint, recovery will safely restore state:
    /// <list type="bullet">
    ///   <item>If crash before flush: recovery replays WAL from the beginning.</item>
    ///   <item>If crash after flush, before WAL truncation: recovery skips already-applied entries.</item>
    ///   <item>If crash after WAL truncation: no recovery needed (state is consistent).</item>
    /// </list>
    /// </para>
    /// </summary>
    public void ForceCheckpoint()
    {
        // Step 1: Flush index region to disk (makes all index entries durable).
        indexRegion.Flush();

        // Step 2: Flush data region to disk (makes all payloads durable).
        dataRegion.Flush();

        // Step 3: Truncate WAL (SetLength(0) + fsync).
        // Safe to truncate ONLY AFTER both regions are flushed.
        // If we truncate before flushing, a crash would lose data.
        wal.Checkpoint();

        // Reset write counter for the next checkpoint interval.
        _writesSinceCheckpoint = 0;
    }
}