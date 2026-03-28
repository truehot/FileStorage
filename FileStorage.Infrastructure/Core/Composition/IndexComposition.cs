using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Indexing.Primary;
using FileStorage.Infrastructure.Indexing.SecondaryIndex;

namespace FileStorage.Infrastructure.Core.Composition;

/// <summary>
/// Composes primary and secondary index services.
/// Responsible for: MemoryIndex, IndexManager, SecondaryIndexManager.
/// </summary>
internal static class IndexComposition
{
    public static (IMemoryIndex Memory, IIndexManager Manager, ISecondaryIndexManager Secondary) CreateIndexServices(
        IRegionProvider regions,
        string basePath,
        int flushThreshold,
        int compactionThreshold)
    {
        ArgumentNullException.ThrowIfNull(regions);
        ArgumentException.ThrowIfNullOrWhiteSpace(basePath);

        IMemoryIndex memoryIndex = new MemoryIndex();
        IIndexManager indexManager = new IndexManager(regions, memoryIndex);
        ISecondaryIndexManager secondaryIndex = new SecondaryIndexManager(
            basePath, flushThreshold, compactionThreshold);

        return (memoryIndex, indexManager, secondaryIndex);
    }
}
