namespace FileStorage.Infrastructure.IO;

/// <summary>
/// Holds the current mmap regions and provides a single place to swap them
/// after compaction. Eliminates scattered field reassignment across the engine.
/// </summary>
internal sealed class RegionProvider : IDisposable
{
    public IMmapRegion IndexRegion { get; private set; }
    public IMmapRegion DataRegion { get; private set; }

    internal RegionProvider(IMmapRegion indexRegion, IMmapRegion dataRegion)
    {
        IndexRegion = indexRegion;
        DataRegion = dataRegion;
    }

    /// <summary>
    /// Replaces a region after compaction. Returns the new region.
    /// Called by <see cref="Compaction.ICompactionService"/> via a callback.
    /// </summary>
    public IMmapRegion Reopen(IMmapRegion oldRegion)
    {
        var old = (MmapRegion)oldRegion;
        var newRegion = new MmapRegion(old.Path, old.InitialSize, old.MaxSize);

        if (ReferenceEquals(oldRegion, IndexRegion))
            IndexRegion = newRegion;
        else if (ReferenceEquals(oldRegion, DataRegion))
            DataRegion = newRegion;

        return newRegion;
    }

    public void Dispose()
    {
        IndexRegion.Dispose();
        DataRegion.Dispose();
    }
}