using System.Buffers.Binary;
using System.Text;
using FileStorage.Infrastructure.Hashing;

namespace FileStorage.Infrastructure.WAL;

/// <summary>
/// Append-only Write-Ahead Log with CRC32 integrity checks and checkpoint support.
///
/// Record layout (on disk):
/// [CRC32:4][SeqNo:8][Op:1][TableLen:4][Table:N][Key:16][DataLen:4][Data:N][DataOffset:8][IndexOffset:8]
///
/// CRC32 covers all bytes after the CRC32 field.
/// </summary>
internal sealed class WriteAheadLog(string path) : IWriteAheadLog, IDisposable
{
    /// <summary>
    /// Maximum allowed data payload size (16 MB).
    /// Prevents OOM from corrupted <c>dataLen</c> fields before CRC validation.
    /// </summary>
    private const int MaxDataLen = 16 * 1024 * 1024;

    private readonly FileStream _stream = new(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read,
        bufferSize: 4096, FileOptions.SequentialScan | FileOptions.WriteThrough);
    private long _sequenceNumber;
    private bool _disposed;

    public long SequenceNumber => _sequenceNumber;

    /// <summary>
    /// Appends a WAL entry, computes CRC32, and fsyncs to disk.
    /// Returns the assigned sequence number.
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
    /// Stops at first corrupted/incomplete record.
    /// <para>
    /// <b>Single-pass design:</b> Each record is read exactly once into a contiguous buffer.
    /// CRC is verified over the already-read bytes — no Seek/re-read.
    /// </para>
    /// </summary>
    public List<WalEntry> ReadAll()
    {
        List<WalEntry> entries = [];
        _stream.Seek(0, SeekOrigin.Begin);

        byte[] headerBuf = new byte[WalEntrySerializer.MinHeaderSize];

        while (_stream.Position < _stream.Length)
        {
            long recordStart = _stream.Position;

            // ── 1. Read fixed header: [CRC:4][SeqNo:8][Op:1][TableLen:4] ──
            if (!TryReadExactly(headerBuf, 0, WalEntrySerializer.MinHeaderSize))
                break;

            if (!WalEntrySerializer.TryReadHeader(headerBuf, out uint storedCrc, out long seqNo, out var op, out int tableLen))
                break;

            // ── 2. Calculate total record size and validate before allocating ──
            int varSize = WalEntrySerializer.VariablePartSize(tableLen);
            // varSize = tableLen + GuidSize(16) + DataLenSize(4)
            // We need to read varSize bytes to learn dataLen, then read data + offsets.

            // Read variable part: [Table:N][Key:16][DataLen:4]
            byte[] varBuf = new byte[varSize];
            if (!TryReadExactly(varBuf, 0, varSize))
                break;

            var (table, key, dataLen) = WalEntrySerializer.ReadVariablePart(varBuf, tableLen);

            // ── 3. Validate dataLen BEFORE allocating ──
            // A corrupted dataLen could be billions — catch it early.
            if (dataLen < 0 || dataLen > MaxDataLen)
                break;

            long remaining = _stream.Length - _stream.Position;
            if (dataLen > remaining - WalEntrySerializer.OffsetTrailerSize)
                break;

            // ── 4. Read data payload + offset trailer ──
            var data = dataLen > 0 ? new byte[dataLen] : [];
            if (dataLen > 0 && !TryReadExactly(data, 0, dataLen))
                break;

            byte[] offsetBuf = new byte[WalEntrySerializer.OffsetTrailerSize];
            if (!TryReadExactly(offsetBuf, 0, offsetBuf.Length))
                break;

            var (dataOffset, indexOffset) = WalEntrySerializer.ReadOffsets(offsetBuf);

            // ── 5. Verify CRC over already-read bytes (no re-read) ──
            // CRC covers everything after the CRC field:
            // [SeqNo:8][Op:1][TableLen:4][Table:N][Key:16][DataLen:4][Data:N][DataOffset:8][IndexOffset:8]
            //
            // We assemble the payload from the buffers we already have in memory.
            int payloadSize = (WalEntrySerializer.MinHeaderSize - 4) // SeqNo + Op + TableLen (minus CRC)
                            + varSize                                 // Table + Key + DataLen
                            + dataLen                                 // Data
                            + WalEntrySerializer.OffsetTrailerSize;   // DataOffset + IndexOffset

            byte[] payload = new byte[payloadSize];
            int pos = 0;

            // Copy header after CRC (SeqNo + Op + TableLen = MinHeaderSize - 4)
            Buffer.BlockCopy(headerBuf, 4, payload, pos, WalEntrySerializer.MinHeaderSize - 4);
            pos += WalEntrySerializer.MinHeaderSize - 4;

            // Copy variable part (Table + Key + DataLen)
            Buffer.BlockCopy(varBuf, 0, payload, pos, varSize);
            pos += varSize;

            // Copy data payload
            if (dataLen > 0)
            {
                Buffer.BlockCopy(data, 0, payload, pos, dataLen);
                pos += dataLen;
            }

            // Copy offset trailer
            Buffer.BlockCopy(offsetBuf, 0, payload, pos, WalEntrySerializer.OffsetTrailerSize);

            if (!WalEntrySerializer.VerifyCrc(payload, storedCrc))
                break;

            // ── 6. Entry is valid — commit it ──
            _sequenceNumber = Math.Max(_sequenceNumber, seqNo);

            entries.Add(new WalEntry
            {
                SequenceNumber = seqNo,
                Operation = op,
                Table = table,
                Key = key,
                Data = data,
                DataOffset = dataOffset,
                IndexOffset = indexOffset
            });
        }

        return entries;
    }

    /// <summary>
    /// Truncates the WAL file after a successful checkpoint.
    /// All entries have been applied to .idx/.dat and flushed.
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