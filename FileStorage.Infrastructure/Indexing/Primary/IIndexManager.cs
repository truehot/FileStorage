namespace FileStorage.Infrastructure.Indexing.Primary;

/// <summary>
/// Manages physical index entries on the mmap region.
/// Owns write position cursors and encapsulates all binary layout knowledge.
/// </summary>
internal interface IIndexManager
{
    /// <summary>Fixed byte size of one index entry.</summary>
    int EntrySize { get; }

    /// <summary>Next write position in the data region.</summary>
    long NextDataOffset { get; }

    /// <summary>Next write position in the index region.</summary>
    long NextIndexOffset { get; }

    /// <summary>Validates that the table name fits in the index entry.</summary>
    void ValidateTableName(string table);

    /// <summary>
    /// Ensures regions have capacity, writes data + index entry, updates in-memory index,
    /// and advances write positions. Returns the offsets used (for WAL recording).
    /// </summary>
    (long DataOffset, long IndexOffset) ApplySave(string table, Guid key, byte[] data);

    /// <summary>
    /// Overload used during WAL replay with pre-determined offsets.
    /// </summary>
    void ApplySave(string table, Guid key, byte[] data, long dataOffset, long indexOffset);

    /// <summary>
    /// Writes data and index entry to disk regions and advances write cursors,
    /// but does not publish to MemoryIndex.
    /// </summary>
    void ApplySavePhysical(string table, Guid key, byte[] data, long dataOffset, long indexOffset);

    /// <summary>
    /// Publishes an already persisted index entry into MemoryIndex.
    /// </summary>
    void PublishSave(string table, Guid key, long indexOffset);

    /// <summary>Marks an index entry as soft-deleted and removes it from the in-memory index.</summary>
    void ApplyDelete(string table, Guid key, long indexOffset);

    /// <summary>
    /// Marks all entries of a table as soft-deleted in the index.
    /// Used by <see cref="WalOperationType.DropTable"/> replay.
    /// </summary>
    void ApplyDropTable(string table);

    /// <summary>
    /// Marks all entries of a table as soft-deleted in the index,
    /// identical to <see cref="ApplyDropTable"/> at the storage level.
    /// The semantic difference (table continues to "exist") is handled
    /// by the caller - the engine simply clears the data.
    /// </summary>
    void ApplyTruncateTable(string table);

    /// <summary>
    /// Scans all index entries and recalculates write positions.
    /// Used after compaction.
    /// </summary>
    void RecalculateWritePositions();

    /// <summary>
    /// Sets write positions directly (used after recovery).
    /// </summary>
    void SetWritePositions(long indexWritePos, long dataWritePos);
}