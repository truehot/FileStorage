namespace FileStorage.Infrastructure.Checkpoint;

/// <summary>
/// Defines the contract for managing checkpoints that flush WAL entries to persistent storage.
/// </summary>
internal interface ICheckpointManager
{
    void TrackWrite();
    void ForceCheckpoint();
}