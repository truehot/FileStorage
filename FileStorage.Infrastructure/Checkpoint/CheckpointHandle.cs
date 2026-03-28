namespace FileStorage.Infrastructure.Checkpoint;

/// <summary>
/// Holds the current checkpoint manager instance used by engine operations.
/// </summary>
internal sealed class CheckpointHandle
{
    private volatile ICheckpointManager _current;

    internal CheckpointHandle(ICheckpointManager current)
    {
        ArgumentNullException.ThrowIfNull(current);
        _current = current;
    }

    /// <summary>
    /// Gets or replaces the current checkpoint manager.
    /// </summary>
    public ICheckpointManager Current
    {
        get => _current;
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            _current = value;
        }
    }
}
