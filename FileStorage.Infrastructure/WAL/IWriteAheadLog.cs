namespace FileStorage.Infrastructure.WAL;

/// <summary>
/// Defines the contract for an append-only Write-Ahead Log with checkpoint support.
/// </summary>
internal interface IWriteAheadLog : IDisposable
{
    long SequenceNumber { get; }
    long Append(WalEntry entry);
    List<WalEntry> ReadAll();
    void Checkpoint();
}