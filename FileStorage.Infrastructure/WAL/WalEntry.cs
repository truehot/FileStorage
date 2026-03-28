namespace FileStorage.Infrastructure.WAL;

/// <summary>
/// WAL operation type.
/// </summary>
internal enum WalOperationType : byte
{
    Save = 1,
    Delete = 2,
    DropTable = 3,
    TruncateTable = 4,
    SaveBatch = 5,
    DeleteBatch = 6 // batch delete operation
}

/// <summary>
/// In-memory representation of a WAL record.
/// </summary>
internal readonly struct WalEntry
{
    public long SequenceNumber { get; init; }
    public WalOperationType Operation { get; init; }
    public string Table { get; init; }
    public Guid Key { get; init; }

    /// <summary>Data payload (for Save or serialized batch payload for SaveBatch).</summary>
    public byte[] Data { get; init; }

    /// <summary>Positions recorded after applying to mmap (used for replay).</summary>
    public long DataOffset { get; init; }
    public long IndexOffset { get; init; }

    /// <summary>
    /// Indexed field values recorded at write time.
    /// Persisted in WAL so secondary indexes can be rebuilt during replay.
    /// Empty for Delete/Drop/Truncate operations.
    /// </summary>
    public IReadOnlyDictionary<string, string> IndexedFields { get; init; }
}