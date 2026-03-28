namespace FileStorage.Infrastructure.Core.IO;

/// <summary>
/// Manages access to memory-mapped file regions.
/// Provides snapshot-based read-safe access and atomic region swapping during compaction.
/// </summary>
internal interface IRegionProvider : IDisposable
{
    /// <summary>
    /// The index region (contains index entries and deletions).
    /// </summary>
    IMmapRegion IndexRegion { get; }

    /// <summary>
    /// The data region (contains record payloads).
    /// </summary>
    IMmapRegion DataRegion { get; }

    /// <summary>
    /// Replaces a region after compaction. Returns the new region.
    /// Called by compaction service via callback.
    /// </summary>
    IMmapRegion Reopen(IMmapRegion oldRegion);
}
