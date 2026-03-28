namespace FileStorage.Infrastructure.WAL;

/// <summary>
/// Append-only Write-Ahead Log with CRC32 integrity checks and checkpoint support.
///
/// <para>
/// <b>Durability model:</b><br/>
/// WAL is the durability boundary: each entry is appended and flushed to disk
/// BEFORE any in-memory index change is visible. This ensures that if a crash occurs,
/// recovery can replay the WAL to restore all committed changes.
/// </para>
///
/// <para>
/// <b>Record layout (on disk):</b><br/>
/// [CRC32:4][SeqNo:8][Op:1][TableLen:4][Table:N][Key:16][DataLen:4][Data:N][DataOffset:8][IndexOffset:8]<br/>
/// CRC32 covers all bytes after the CRC32 field, protecting against corruption.
/// </para>
///
/// <para>
/// <b>Checkpoint safety:</b><br/>
/// <see cref="Checkpoint"/> truncates the WAL after regions are flushed to disk.
/// This preserves the invariant: WAL contains only uncommitted changes.
/// The checkpoint must be called in strict order: flush index → flush data → truncate WAL.
/// </para>
/// </summary>
internal sealed class WriteAheadLog(string path) : IWriteAheadLog, IDisposable
{
    /// <summary>
    /// Maximum allowed data payload size (16 MB).
    /// Prevents OOM from corrupted <c>dataLen</c> fields before CRC validation.
    /// </summary>
    private const int MaxDataLen = 16 * 1024 * 1024;
    private const int MaxBatchDataLen = 128 * 1024 * 1024;

    private readonly FileStream _stream = new(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read,
        bufferSize: 4096, FileOptions.SequentialScan | FileOptions.WriteThrough);
    private long _sequenceNumber;
    private bool _disposed;

    public long SequenceNumber => _sequenceNumber;

    /// <summary>
    /// Appends a WAL entry, computes CRC32, and fsyncs to disk.
    /// <para>
    /// <b>Critical invariant:</b><br/>
    /// This method MUST return successfully before the corresponding index/data change
    /// is considered committed. This ensures durability: if a crash occurs, recovery
    /// can replay the WAL entry to restore the change.
    /// </para>
    /// </summary>
    public long Append(WalEntry entry)
    {
        _sequenceNumber++;
        var seqNo = _sequenceNumber;

        byte[] record = WalEntrySerializer.Serialize(entry, seqNo);

        _stream.Seek(0, SeekOrigin.End);
        _stream.Write(record);
        _stream.Flush(flushToDisk: true);

        return seqNo;
    }

    /// <summary>
    /// Reads all valid WAL entries (with correct CRC) for replay.
    /// Stops at first corrupted/incomplete record and truncates invalid tail.
    ///
    /// <para>
    /// <b>Single-pass design with tail protection:</b><br/>
    /// Each record is read exactly once into a contiguous buffer. CRC is verified
    /// over the already-read bytes — no Seek/re-read. If any record is corrupted
    /// or incomplete, reading stops and the invalid tail is automatically truncated.
    /// </para>
    ///
    /// <para>
    /// <b>Corruption detection strategy:</b><br/>
    /// Reading stops at the first sign of corruption:
    /// <list type="bullet">
    ///   <item>Fixed header incomplete: truncate tail.</item>
    ///   <item>Variable part incomplete: truncate tail.</item>
    ///   <item>Data length invalid (negative or exceeds max): truncate tail.</item>
    ///   <item>Data payload incomplete: truncate tail.</item>
    ///   <item>Offset trailer incomplete: truncate tail.</item>
    ///   <item>CRC mismatch: truncate tail.</item>
    /// </list>
    /// In all cases, the tail is truncated to <c>lastGoodPos</c> (last valid record),
    /// ensuring only valid entries are returned and on-disk state is consistent.
    /// </para>
    ///
    /// <para>
    /// <b>Replay safety:</b><br/>
    /// Returned entries are guaranteed to have:
    /// <list type="bullet">
    ///   <item>Valid CRC32 (corruption detected).</item>
    ///   <item>Complete header, variable part, and trailer.</item>
    ///   <item>Valid data length (positive, within limits).</item>
    /// </list>
    /// These entries can be safely replayed for recovery without additional validation.
    /// </para>
    /// </summary>
    public List<WalEntry> ReadAll() => [.. ReadAllStreaming()];

    /// <summary>
    /// Streams valid WAL entries (with correct CRC) for replay.
    /// Stops at first corrupted/incomplete record and truncates invalid tail.
    ///
    /// <para>
    /// <b>Idempotency guarantee:</b><br/>
    /// If recovery is called multiple times:
    /// <list type="number">
    ///   <item>First run: reads entries, validates, truncates invalid tail.</item>
    ///   <item>Subsequent runs: same entries returned (valid tail is stable).</item>
    /// </list>
    /// The invalid tail is truncated only once. Repeated calls return the same entries.
    /// </para>
    /// </summary>
    public IEnumerable<WalEntry> ReadAllStreaming()
    {
        _stream.Seek(0, SeekOrigin.Begin);

        long lastGoodPos = 0;
        byte[] headerBuf = new byte[WalEntrySerializer.MinHeaderSize];
        bool truncateTail = false;

        try
        {
            while (_stream.Position < _stream.Length)
            {
                // ── 1. Read fixed header: [CRC:4][SeqNo:8][Op:1][TableLen:4] ──
                if (!TryReadExactly(headerBuf, 0, WalEntrySerializer.MinHeaderSize))
                {
                    truncateTail = true;
                    yield break;
                }

                if (!WalEntrySerializer.TryReadHeader(headerBuf, out uint storedCrc, out long seqNo, out var op, out int tableLen))
                {
                    truncateTail = true;
                    yield break;
                }

                // ── 2. Calculate total record size and validate before allocating ──
                int varSize = WalEntrySerializer.VariablePartSize(tableLen);

                // Read variable part: [Table:N][Key:16][DataLen:4]
                byte[] varBuf = new byte[varSize];
                if (!TryReadExactly(varBuf, 0, varSize))
                {
                    truncateTail = true;
                    yield break;
                }

                var (table, key, dataLen) = WalEntrySerializer.ReadVariablePart(varBuf, tableLen);

                // ── 3. Validate dataLen BEFORE allocating ──
                int maxDataLen = op == WalOperationType.SaveBatch ? MaxBatchDataLen : MaxDataLen;
                if (dataLen < 0 || dataLen > maxDataLen)
                {
                    truncateTail = true;
                    yield break;
                }

                long remaining = _stream.Length - _stream.Position;
                if (dataLen + WalEntrySerializer.OffsetTrailerSize > remaining)
                {
                    truncateTail = true;
                    yield break;
                }

                // ── 4. Read data payload + offset trailer ──
                var data = dataLen > 0 ? new byte[dataLen] : [];
                if (dataLen > 0 && !TryReadExactly(data, 0, dataLen))
                {
                    truncateTail = true;
                    yield break;
                }

                byte[] offsetBuf = new byte[WalEntrySerializer.OffsetTrailerSize];
                if (!TryReadExactly(offsetBuf, 0, offsetBuf.Length))
                {
                    truncateTail = true;
                    yield break;
                }

                var (dataOffset, indexOffset) = WalEntrySerializer.ReadOffsets(offsetBuf);

                // ── 5. Verify CRC over already-read bytes ──
                if (!WalEntrySerializer.VerifyCrc(
                        headerBuf.AsSpan(4, WalEntrySerializer.MinHeaderSize - 4),
                        varBuf,
                        data,
                        offsetBuf,
                        storedCrc))
                {
                    truncateTail = true;
                    yield break;
                }

                // ── 6. Entry is valid — commit it ──
                _sequenceNumber = Math.Max(_sequenceNumber, seqNo);
                lastGoodPos = _stream.Position;

                yield return new WalEntry
                {
                    SequenceNumber = seqNo,
                    Operation = op,
                    Table = table,
                    Key = key,
                    Data = data,
                    DataOffset = dataOffset,
                    IndexOffset = indexOffset
                };
            }
        }
        finally
        {
            // Truncate invalid tail to ensure next read sees only valid entries.
            // This preserves the invariant: all entries in WAL are valid and recoverable.
            if (truncateTail && lastGoodPos < _stream.Length)
            {
                _stream.SetLength(lastGoodPos);
                _stream.Flush(flushToDisk: true);
            }
        }
    }

    /// <summary>
    /// Truncates the WAL file after a successful checkpoint.
    /// <para>
    /// <b>Critical ordering:</b><br/>
    /// This method MUST be called ONLY AFTER:
    /// <list type="number">
    ///   <item>Index region is flushed to disk (fsync).</item>
    ///   <item>Data region is flushed to disk (fsync).</item>
    /// </list>
    /// If <see cref="Checkpoint"/> is called before regions are flushed,
    /// a crash will lose all uncommitted writes.
    /// </para>
    ///
    /// <para>
    /// <b>Safety guarantee:</b><br/>
    /// After this method returns, the WAL is empty. All entries that were in the WAL
    /// are now durable on disk (in index/data regions). The invariant is preserved:
    /// WAL contains only uncommitted changes (none, after checkpoint).
    /// </para>
    /// </summary>
    public void Checkpoint()
    {
        _stream.SetLength(0);
        _stream.Flush(flushToDisk: true);
    }

    private bool TryReadExactly(byte[] buffer, int offset, int count)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            int read = _stream.Read(buffer, offset + totalRead, count - totalRead);
            if (read == 0) return false;
            totalRead += read;
        }
        return true;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _stream?.Dispose();
    }
}