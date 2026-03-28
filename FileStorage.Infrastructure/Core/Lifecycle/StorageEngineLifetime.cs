namespace FileStorage.Infrastructure.Core.Lifecycle;

/// <summary>
/// Coordinates storage engine lifetime transitions and fail-fast checks.
/// </summary>
internal sealed class StorageEngineLifetime
{
    private enum EngineState
    {
        Active = 0,
        Disposing = 1,
        Disposed = 2
    }

    private int _state = (int)EngineState.Active;

    /// <summary>
    /// Throws when the engine is no longer active.
    /// </summary>
    public void ThrowIfNotActive(Type disposedType)
    {
        ObjectDisposedException.ThrowIf(
            Volatile.Read(ref _state) != (int)EngineState.Active,
            disposedType);
    }

    /// <summary>
    /// Transitions the engine into disposing state once.
    /// </summary>
    public bool TryBeginDispose() =>
        Interlocked.CompareExchange(
            ref _state,
            (int)EngineState.Disposing,
            (int)EngineState.Active) == (int)EngineState.Active;

    /// <summary>
    /// Marks the engine as fully disposed.
    /// </summary>
    public void MarkDisposed()
    {
        Volatile.Write(ref _state, (int)EngineState.Disposed);
    }
}