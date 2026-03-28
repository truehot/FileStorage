using FileStorage.Infrastructure.Indexing.SecondaryIndex;
using FileStorage.Infrastructure.WAL;

namespace FileStorage.Infrastructure.Recovery;

/// <summary>
/// Replays WAL entries required to rebuild secondary-index state.
/// </summary>
internal sealed class SecondaryIndexReplayService
{
    private readonly IWriteAheadLog _wal;
    private readonly ISecondaryIndexManager _secondaryIndex;

    internal SecondaryIndexReplayService(IWriteAheadLog wal, ISecondaryIndexManager secondaryIndex)
    {
        ArgumentNullException.ThrowIfNull(wal);
        ArgumentNullException.ThrowIfNull(secondaryIndex);

        _wal = wal;
        _secondaryIndex = secondaryIndex;
    }

    /// <summary>
    /// Replays secondary-index mutations from the WAL.
    /// </summary>
    public void Replay()
    {
        foreach (var entry in _wal.ReadAllStreaming())
        {
            switch (entry.Operation)
            {
                case WalOperationType.Save when entry.IndexedFields is { Count: > 0 }:
                    _secondaryIndex.Put(entry.Table, entry.Key, entry.IndexedFields);
                    break;

                case WalOperationType.SaveBatch:
                    if (WalBatchPayloadSerializer.TryDeserialize(entry.Data, out var batchEntries))
                    {
                        var secondaryBatch = new List<(Guid RecordKey, IReadOnlyDictionary<string, string> IndexedFields)>(batchEntries.Count);
                        foreach (var batchEntry in batchEntries)
                            secondaryBatch.Add((batchEntry.Key, batchEntry.IndexedFields));

                        _secondaryIndex.PutRange(entry.Table, secondaryBatch);
                    }
                    break;

                case WalOperationType.Delete:
                    _secondaryIndex.RemoveByKey(entry.Table, entry.Key);
                    break;

                case WalOperationType.DropTable:
                case WalOperationType.TruncateTable:
                    _secondaryIndex.DropAllIndexes(entry.Table);
                    break;
            }
        }
    }
}
