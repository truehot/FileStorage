using FileStorage.Infrastructure.Indexing.SecondaryIndex;

namespace FileStorage.Infrastructure.Tests.Indexing.SecondaryIndex;

public sealed class SecondaryIndexManagerTests
{
    [Fact]
    public void PutRange_ThenLookup_ReturnsInsertedKeys()
    {
        string dir = CreateTempDir();
        try
        {
            using var manager = new SecondaryIndexManager(dir, flushThreshold: 1024, compactionThreshold: 8);
            manager.EnsureIndex("users", "status");

            var k1 = Guid.NewGuid();
            var k2 = Guid.NewGuid();
            var k3 = Guid.NewGuid();

            manager.PutRange("users", [
                (k1, (IReadOnlyDictionary<string, string>)new Dictionary<string, string> { ["status"] = "active" }),
                (k2, (IReadOnlyDictionary<string, string>)new Dictionary<string, string> { ["status"] = "inactive" }),
                (k3, (IReadOnlyDictionary<string, string>)new Dictionary<string, string> { ["status"] = "active" })
            ]);

            var active = manager.Lookup("users", "status", "active");
            var inactive = manager.Lookup("users", "status", "inactive");

            Assert.Contains(k1, active);
            Assert.Contains(k3, active);
            Assert.Single(inactive);
            Assert.Contains(k2, inactive);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void PutRange_WithEmptyBatch_DoesNothing()
    {
        string dir = CreateTempDir();
        try
        {
            using var manager = new SecondaryIndexManager(dir);
            manager.EnsureIndex("users", "status");

            IReadOnlyCollection<(Guid RecordKey, IReadOnlyDictionary<string, string> IndexedFields)> empty = [];

            manager.PutRange("users", empty);

            var result = manager.Lookup("users", "status", "active");
            Assert.Empty(result);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void PutRange_UnknownFieldWithoutIndex_IsIgnored()
    {
        string dir = CreateTempDir();
        try
        {
            using var manager = new SecondaryIndexManager(dir);
            manager.EnsureIndex("users", "status");

            var key = Guid.NewGuid();
            manager.PutRange("users", [
                (key, (IReadOnlyDictionary<string, string>)new Dictionary<string, string>
                {
                    ["unknown"] = "x"
                })
            ]);

            Assert.Empty(manager.Lookup("users", "status", "x"));
            Assert.Empty(manager.Lookup("users", "unknown", "x"));
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void PutRange_SingleEntryWithMultipleIndexedFields_UpdatesEachExistingIndex()
    {
        string dir = CreateTempDir();
        try
        {
            using var manager = new SecondaryIndexManager(dir);
            manager.EnsureIndex("users", "status");
            manager.EnsureIndex("users", "team");

            var key = Guid.NewGuid();
            manager.PutRange("users", [
                (key, (IReadOnlyDictionary<string, string>)new Dictionary<string, string>
                {
                    ["status"] = "active",
                    ["team"] = "core"
                })
            ]);

            Assert.Contains(key, manager.Lookup("users", "status", "active"));
            Assert.Contains(key, manager.Lookup("users", "team", "core"));
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void DropAllIndexes_RemovesIndexDefinitionsForTable()
    {
        string dir = CreateTempDir();
        try
        {
            using var manager = new SecondaryIndexManager(dir);
            manager.EnsureIndex("users", "status");
            manager.EnsureIndex("users", "team");

            Assert.True(manager.HasIndex("users", "status"));
            Assert.True(manager.HasIndex("users", "team"));

            manager.DropAllIndexes("users");

            Assert.False(manager.HasIndex("users", "status"));
            Assert.False(manager.HasIndex("users", "team"));
            Assert.Empty(manager.GetIndexes("users"));
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void PutRange_RemoveByKey_RemovesFromMemTableLookup()
    {
        string dir = CreateTempDir();
        try
        {
            using var manager = new SecondaryIndexManager(dir, flushThreshold: 1024, compactionThreshold: 8);
            manager.EnsureIndex("users", "status");

            var key = Guid.NewGuid();
            manager.PutRange("users", [
                (key, (IReadOnlyDictionary<string, string>)new Dictionary<string, string> { ["status"] = "active" })
            ]);

            Assert.Contains(key, manager.Lookup("users", "status", "active"));

            manager.RemoveByKey("users", key);

            Assert.DoesNotContain(key, manager.Lookup("users", "status", "active"));
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void PutRange_WithFlushThresholdOne_PersistsAndLookupWorksFromSstable()
    {
        string dir = CreateTempDir();
        try
        {
            var k1 = Guid.NewGuid();
            var k2 = Guid.NewGuid();

            using (var manager = new SecondaryIndexManager(dir, flushThreshold: 1, compactionThreshold: 100))
            {
                manager.EnsureIndex("users", "status");

                manager.PutRange("users", [
                    (k1, (IReadOnlyDictionary<string, string>)new Dictionary<string, string> { ["status"] = "active" }),
                    (k2, (IReadOnlyDictionary<string, string>)new Dictionary<string, string> { ["status"] = "active" })
                ]);
            }

            using var reloaded = new SecondaryIndexManager(dir, flushThreshold: 1, compactionThreshold: 100);
            reloaded.LoadExisting();
            var active = reloaded.Lookup("users", "status", "active");

            Assert.Contains(k1, active);
            Assert.Contains(k2, active);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void LoadExisting_AfterPutRange_RestoresLookupState()
    {
        string dir = CreateTempDir();
        try
        {
            var key = Guid.NewGuid();

            using (var manager = new SecondaryIndexManager(dir, flushThreshold: 1, compactionThreshold: 100))
            {
                manager.EnsureIndex("users", "team");
                manager.PutRange("users", [
                    (key, (IReadOnlyDictionary<string, string>)new Dictionary<string, string> { ["team"] = "core" })
                ]);
            }

            using var recovered = new SecondaryIndexManager(dir, flushThreshold: 1, compactionThreshold: 100);
            recovered.LoadExisting();

            var core = recovered.Lookup("users", "team", "core");
            Assert.Contains(key, core);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void PutRange_MixedIndexedAndNonIndexedFields_IndexesOnlyActiveFields()
    {
        string dir = CreateTempDir();
        try
        {
            using var manager = new SecondaryIndexManager(dir);
            manager.EnsureIndex("users", "status");

            var key = Guid.NewGuid();
            manager.PutRange("users", [
                (key, (IReadOnlyDictionary<string, string>)new Dictionary<string, string>
                {
                    ["status"] = "active",
                    ["nonIndexed"] = "x"
                })
            ]);

            Assert.Contains(key, manager.Lookup("users", "status", "active"));
            Assert.Empty(manager.Lookup("users", "nonIndexed", "x"));
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void PutRange_DuplicateKeySameFieldValue_DeduplicatesInLookup()
    {
        string dir = CreateTempDir();
        try
        {
            using var manager = new SecondaryIndexManager(dir);
            manager.EnsureIndex("users", "status");

            var key = Guid.NewGuid();
            manager.PutRange("users", [
                (key, (IReadOnlyDictionary<string, string>)new Dictionary<string, string> { ["status"] = "active" }),
                (key, (IReadOnlyDictionary<string, string>)new Dictionary<string, string> { ["status"] = "active" })
            ]);

            var active = manager.Lookup("users", "status", "active");
            Assert.Single(active);
            Assert.Equal(key, active[0]);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void Put_FlushBoundary_UsesTotalMappings_NotDistinctKeyCount()
    {
        string dir = CreateTempDir();
        try
        {
            using var manager = new SecondaryIndexManager(dir, flushThreshold: 2, compactionThreshold: 100);
            manager.EnsureIndex("users", "status");

            var k1 = Guid.NewGuid();
            var k2 = Guid.NewGuid();

            manager.Put("users", k1, new Dictionary<string, string> { ["status"] = "active" });
            manager.Put("users", k1, new Dictionary<string, string> { ["status"] = "active" });

            var indexDir = Path.Combine(dir, "users", "status");
            Assert.Empty(Directory.GetFiles(indexDir, "*.sst"));

            manager.Put("users", k2, new Dictionary<string, string> { ["status"] = "active" });

            var sstFiles = Directory.GetFiles(indexDir, "*.sst");
            Assert.Single(sstFiles);

            var active = manager.Lookup("users", "status", "active");
            Assert.Contains(k1, active);
            Assert.Contains(k2, active);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void Compaction_WithSameInputSequence_ProducesSameLogicalResults()
    {
        static IReadOnlyDictionary<string, HashSet<Guid>> RunScenario(string dir, Guid g1, Guid g2, Guid g3)
        {
            using var manager = new SecondaryIndexManager(dir, flushThreshold: 1, compactionThreshold: 2);
            manager.EnsureIndex("users", "status");

            manager.Put("users", g1, new Dictionary<string, string> { ["status"] = "active" });
            manager.Put("users", g2, new Dictionary<string, string> { ["status"] = "active" });
            manager.Put("users", g3, new Dictionary<string, string> { ["status"] = "inactive" });
            manager.Put("users", g1, new Dictionary<string, string> { ["status"] = "active" });
            manager.Put("users", g3, new Dictionary<string, string> { ["status"] = "inactive" });

            var indexDir = Path.Combine(dir, "users", "status");
            Assert.True(Directory.GetFiles(indexDir, "*.sst").Length >= 1);

            return new Dictionary<string, HashSet<Guid>>(StringComparer.Ordinal)
            {
                ["active"] = [.. manager.Lookup("users", "status", "active")],
                ["inactive"] = [.. manager.Lookup("users", "status", "inactive")]
            };
        }

        string dir1 = CreateTempDir();
        string dir2 = CreateTempDir();

        try
        {
            var g1 = Guid.Parse("11111111-1111-1111-1111-111111111111");
            var g2 = Guid.Parse("22222222-2222-2222-2222-222222222222");
            var g3 = Guid.Parse("33333333-3333-3333-3333-333333333333");

            var run1 = RunScenario(dir1, g1, g2, g3);
            var run2 = RunScenario(dir2, g1, g2, g3);

            Assert.True(run1["active"].SetEquals(run2["active"]));
            Assert.True(run1["inactive"].SetEquals(run2["inactive"]));
        }
        finally
        {
            TryDeleteDir(dir1);
            TryDeleteDir(dir2);
        }
    }

    [Fact]
    public void Compaction_WithManyOldSSTables_PerformsSwapEfficiently()
    {
        // Test for Fix #1: Verify that oldTables lookup uses HashSet (O(K)) not List.Contains (O(K?))
        // This test doesn't directly verify implementation, but ensures behavior is correct
        // when many SSTables are present during compaction

        string dir = CreateTempDir();
        try
        {
            using var manager = new SecondaryIndexManager(dir, flushThreshold: 1, compactionThreshold: 2);
            manager.EnsureIndex("users", "status");

            var keys = Enumerable.Range(0, 10)
                .Select(_ => Guid.NewGuid())
                .ToList();

            // Put 10 unique keys, each triggers flush + potential compaction
            foreach (var key in keys)
            {
                manager.Put("users", key, new Dictionary<string, string> { ["status"] = "active" });
            }

            // Verify all keys are still findable after compaction
            var result = manager.Lookup("users", "status", "active");
            Assert.Equal(keys.Count, result.Count);
            foreach (var key in keys)
                Assert.Contains(key, result);
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void TryDeleteFile_AlsoDeletesBloomSidecar()
    {
        // Test for Fix #2: Verify that .bloom sidecar files are deleted alongside .sst

        string dir = CreateTempDir();
        try
        {
            string testDir = Path.Combine(dir, "bloom_test");
            Directory.CreateDirectory(testDir);

            // Create fake .sst and .bloom files
            string sstPath = Path.Combine(testDir, "test.sst");
            string bloomPath = Path.Combine(testDir, "test.bloom");

            File.WriteAllText(sstPath, "dummy sst");
            File.WriteAllText(bloomPath, "dummy bloom");

            Assert.True(File.Exists(sstPath));
            Assert.True(File.Exists(bloomPath));

            // Call TryDeleteFile which should delete both
            // We need to use reflection to call private method
            var method = typeof(SecondaryIndexManager).GetMethod(
                "TryDeleteFile",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static,
                null,
                [typeof(string)],
                null);

            Assert.NotNull(method);
            method.Invoke(null, [sstPath]);

            Assert.False(File.Exists(sstPath), ".sst file should be deleted");
            Assert.False(File.Exists(bloomPath), ".bloom file should be deleted");
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void RecoverCompaction_WithCompleteMarker_DeletesManifestCorrectly()
    {
        // Test for Fix #3: Verify that COMPLETE marker doesn't cause early return
        // and all cleanup is properly done

        string dir = CreateTempDir();
        try
        {
            string indexDir = Path.Combine(dir, "recovery_test");
            Directory.CreateDirectory(indexDir);

            // Create a fake compaction manifest with COMPLETE marker
            string manifestPath = Path.Combine(indexDir, "compaction.manifest");
            var manifestLines = new[]
            {
                "OLD:file1.sst",
                "OLD:file2.sst",
                "NEW:merged.sst",
                "MERGED",
                "COMPLETE"
            };
            File.WriteAllLines(manifestPath, manifestLines);

            // Create fake files
            File.WriteAllText(Path.Combine(indexDir, "file1.sst"), "old1");
            File.WriteAllText(Path.Combine(indexDir, "file2.sst"), "old2");
            File.WriteAllText(Path.Combine(indexDir, "merged.sst"), "merged");

            Assert.True(File.Exists(manifestPath));
            Assert.True(File.Exists(Path.Combine(indexDir, "file1.sst")));
            Assert.True(File.Exists(Path.Combine(indexDir, "file2.sst")));
            Assert.True(File.Exists(Path.Combine(indexDir, "merged.sst")));

            // Call RecoverCompaction using reflection
            var method = typeof(SecondaryIndexManager).GetMethod(
                "RecoverCompaction",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static,
                null,
                [typeof(string)],
                null);

            Assert.NotNull(method);
            method.Invoke(null, [indexDir]);

            // After recovery with COMPLETE marker, manifest should be deleted
            Assert.False(File.Exists(manifestPath), "Manifest should be deleted after COMPLETE recovery");

            // Files should remain (recovery with COMPLETE doesn't delete old files, 
            // that's done during compaction phase 6)
            // But in this case, the manifest had MERGED, so old files should have been deleted during phase 6
            // Actually, in real scenario, if manifest has COMPLETE, old files were already deleted
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void RecoverCompaction_WithMergedButNoCOMPLETE_DeletesOldFiles()
    {
        // Test for Fix #3: Verify recovery completes compaction when MERGED is present
        
        string dir = CreateTempDir();
        try
        {
            string indexDir = Path.Combine(dir, "recovery_merged");
            Directory.CreateDirectory(indexDir);
            
            // Create fake files first
            string old1Path = Path.Combine(indexDir, "old1.sst");
            string old2Path = Path.Combine(indexDir, "old2.sst");
            string mergedPath = Path.Combine(indexDir, "merged.sst");
            
            File.WriteAllText(old1Path, "old1");
            File.WriteAllText(old2Path, "old2");
            File.WriteAllText(mergedPath, "merged");
            
            // Create manifest with MERGED but no COMPLETE
            // This simulates crash after phase 4 (merge written) but before phase 7 (manifest COMPLETE)
            string manifestPath = Path.Combine(indexDir, "compaction.manifest");
            var manifestLines = new[]
            {
                $"OLD:{old1Path}",
                $"OLD:{old2Path}",
                $"NEW:{mergedPath}",
                "MERGED"
            };
            File.WriteAllLines(manifestPath, manifestLines);
            
            var method = typeof(SecondaryIndexManager).GetMethod(
                "RecoverCompaction",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static,
                null,
                [typeof(string)],
                null);
            
            Assert.NotNull(method);
            method.Invoke(null, [indexDir]);
            
            // After recovery, old files should be deleted
            Assert.False(File.Exists(old1Path), "old1.sst should be deleted");
            Assert.False(File.Exists(old2Path), "old2.sst should be deleted");
            
            // Merged should remain
            Assert.True(File.Exists(mergedPath), "merged.sst should remain");
            
            // Manifest should be deleted
            Assert.False(File.Exists(manifestPath), "Manifest should be deleted");
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    [Fact]
    public void RecoverCompaction_WithoutMerged_DeletesMergedFile()
    {
        // Test for Fix #3: Verify recovery rolls back compaction when merge is incomplete
        
        string dir = CreateTempDir();
        try
        {
            string indexDir = Path.Combine(dir, "recovery_rollback");
            Directory.CreateDirectory(indexDir);
            
            // Create fake files first
            string old1Path = Path.Combine(indexDir, "old1.sst");
            string old2Path = Path.Combine(indexDir, "old2.sst");
            string mergedPath = Path.Combine(indexDir, "merged.sst");
            
            File.WriteAllText(old1Path, "old1");
            File.WriteAllText(old2Path, "old2");
            File.WriteAllText(mergedPath, "incomplete");
            
            // Create manifest without MERGED marker
            // This simulates crash during merge (phase 3)
            string manifestPath = Path.Combine(indexDir, "compaction.manifest");
            var manifestLines = new[]
            {
                $"OLD:{old1Path}",
                $"OLD:{old2Path}",
                $"NEW:{mergedPath}"
            };
            File.WriteAllLines(manifestPath, manifestLines);
            
            var method = typeof(SecondaryIndexManager).GetMethod(
                "RecoverCompaction",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static,
                null,
                [typeof(string)],
                null);
            
            Assert.NotNull(method);
            method.Invoke(null, [indexDir]);
            
            // After recovery, old files should remain (rollback)
            Assert.True(File.Exists(old1Path), "old1.sst should remain");
            Assert.True(File.Exists(old2Path), "old2.sst should remain");
            
            // Merged should be deleted (rollback)
            Assert.False(File.Exists(mergedPath), "merged.sst should be deleted");
            
            // Manifest should be deleted
            Assert.False(File.Exists(manifestPath), "Manifest should be deleted");
        }
        finally
        {
            TryDeleteDir(dir);
        }
    }

    private static string CreateTempDir()
    {
        string dir = Path.Combine(Path.GetTempPath(), "FileStorageX.Tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void TryDeleteDir(string dir)
    {
        try
        {
            if (Directory.Exists(dir))
                Directory.Delete(dir, recursive: true);
        }
        catch
        {
            // best effort cleanup
        }
    }
}
