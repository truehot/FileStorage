using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.IO;

namespace FileStorage.Infrastructure.Compaction;

/// <summary>
/// Reclaims disk space by removing soft-deleted records from index and data files.
/// </summary>
internal interface ICompactionService
{
    /// <summary>
    /// Compacts storage by writing live records to temporary files
    /// and atomically replacing the originals.
    /// Returns the number of dead records removed.
    /// <para>
    /// <b>IMPORTANT — Caller contract:</b> This method disposes and reopens the provided
    /// <paramref name="indexRegion"/> and <paramref name="dataRegion"/>. The caller
    /// <b>MUST</b> hold an exclusive write lock for the entire duration of this call
    /// to prevent concurrent readers from accessing disposed regions.
    /// <c>StorageEngine.CompactAsync</c> satisfies this via <c>AsyncReaderWriterLock.AcquireWriteLockAsync</c>.
    /// </para>
    /// </summary>
    long Compact(
        IMmapRegion indexRegion,
        IMmapRegion dataRegion,
        IMemoryIndex memoryIndex,
        Func<IMmapRegion, IMmapRegion> reopenRegion,
        IReadOnlySet<string>? tables = null);
}