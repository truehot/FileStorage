using FileStorage.Infrastructure.IO;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure.Checkpoint;

internal sealed class CheckpointManager(
    IMmapRegion indexRegion,
    IMmapRegion dataRegion,
    IWriteAheadLog wal,
    int threshold = 1000) : ICheckpointManager
{
    private int _writesSinceCheckpoint;

    /// <summary>
    /// Tracks a write. Automatically flushes and checkpoints when threshold is reached.
    /// </summary>
    public void TrackWrite()
    {
        _writesSinceCheckpoint++;
        if (_writesSinceCheckpoint >= threshold)
            ForceCheckpoint();
    }

    /// <summary>
    /// Forces an immediate checkpoint regardless of threshold.
    /// </summary>
    public void ForceCheckpoint()
    {
        indexRegion.Flush();
        dataRegion.Flush();
        wal.Checkpoint();
        _writesSinceCheckpoint = 0;
    }
}