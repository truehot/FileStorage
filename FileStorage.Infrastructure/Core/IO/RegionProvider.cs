using FileStorage.Infrastructure.Core.IO;

namespace FileStorage.Infrastructure.Core.IO;

/// <summary>
/// Holds the current mmap regions and provides a single place to swap them
/// after compaction. Eliminates scattered field reassignment across the engine.
/// </summary>
internal sealed class RegionProvider : IRegionProvider
{
    public IMmapRegion IndexRegion { get; private set; }
    public IMmapRegion DataRegion { get; private set; }

    internal RegionProvider(IMmapRegion indexRegion, IMmapRegion dataRegion)
    {
        IndexRegion = indexRegion;
        DataRegion = dataRegion;
    }

    /// <summary>
    /// Replaces a region after compaction. Disposes the old region and returns the new one.
    /// Called by <see cref="Compaction.ICompactionService"/> via a callback.
    /// </summary>
    public IMmapRegion Reopen(IMmapRegion oldRegion)
    {
        ArgumentNullException.ThrowIfNull(oldRegion);

        var newRegion = new MmapRegion(oldRegion.Path, oldRegion.InitialSize, oldRegion.MaxSize);

        if (ReferenceEquals(oldRegion, IndexRegion))
            IndexRegion = newRegion;
        else if (ReferenceEquals(oldRegion, DataRegion))
            DataRegion = newRegion;

        oldRegion.Dispose();

        return newRegion;
    }

    public void Dispose()
    {
        IndexRegion.Dispose();
        DataRegion.Dispose();
    }
}