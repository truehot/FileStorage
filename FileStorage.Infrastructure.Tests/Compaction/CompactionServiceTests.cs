using FileStorage.Infrastructure.Compaction;
using FileStorage.Infrastructure.Core.IO;
using FileStorage.Infrastructure.Core.Serialization;
using FileStorage.Infrastructure.Indexing.Primary;

namespace FileStorage.Infrastructure.Tests.Compaction;

public sealed class CompactionServiceTests
{
    [Fact]
    public void Compact_WhenLatestVersionIsDeleted_DoesNotResurrectOlderVersion()
    {
        string root = Path.Combine(Path.GetTempPath(), "filestoragex-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);

        string indexPath = Path.Combine(root, "storage.idx");
        string dataPath = Path.Combine(root, "storage.dat");

        var indexRegion = new MmapRegion(indexPath, 64 * 1024, 8 * 1024 * 1024);
        var dataRegion = new MmapRegion(dataPath, 64 * 1024, 8 * 1024 * 1024);
        var regions = new RegionProvider(indexRegion, dataRegion);

        try
        {
            var memoryIndex = new MemoryIndex();
            var indexManager = new IndexManager(regions, memoryIndex);
            indexManager.SetWritePositions(4096, 0);

            var key = Guid.NewGuid();
            indexManager.ApplySave("users", key, [1, 2, 3], dataOffset: 0, indexOffset: 4096);
            indexManager.PublishSave("users", key, 4096);

            long secondIndexOffset = 4096 + IndexEntrySerializer.EntryFixedSize;
            indexManager.ApplySave("users", key, [4, 5, 6], dataOffset: 3, indexOffset: secondIndexOffset);
            indexManager.PublishSave("users", key, secondIndexOffset);

            Assert.True(memoryIndex.TryGet("users", key, out long latestOffset));
            Assert.Equal(secondIndexOffset, latestOffset);

            indexManager.ApplyDelete("users", key, latestOffset);
            Assert.False(memoryIndex.TryGet("users", key, out _));

            var compaction = new CompactionService();
            long removed = compaction.Compact(
                regions.IndexRegion,
                regions.DataRegion,
                memoryIndex,
                regions.Reopen);

            Assert.True(removed > 0);
            Assert.False(memoryIndex.TryGet("users", key, out _));
            Assert.Equal(0L, memoryIndex.CountByTable("users"));

            bool foundLive = ContainsLiveRecord(regions.IndexRegion, "users", key);
            Assert.False(foundLive);
        }
        finally
        {
            regions.Dispose();
            TryDeleteDirectory(root);
        }
    }

    private static bool ContainsLiveRecord(IMmapRegion indexRegion, string table, Guid key)
    {
        long pos = 4096;
        var buffer = new byte[IndexEntrySerializer.EntryFixedSize];

        while (pos + IndexEntrySerializer.EntryFixedSize <= indexRegion.FileSize)
        {
            indexRegion.Read(pos, buffer, 0, IndexEntrySerializer.EntryFixedSize);
            var span = buffer.AsSpan(0, IndexEntrySerializer.EntryFixedSize);

            if (IndexEntrySerializer.IsEmpty(span))
                break;

            if (!IndexEntrySerializer.IsDeleted(span) &&
                IndexEntrySerializer.ReadKey(span) == key &&
                IndexEntrySerializer.TableEquals(span, table))
            {
                return true;
            }

            pos += IndexEntrySerializer.EntryFixedSize;
        }

        return false;
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
        }
    }
}
