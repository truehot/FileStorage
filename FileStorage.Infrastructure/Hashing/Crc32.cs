namespace FileStorage.Infrastructure.Hashing;

/// <summary>
/// Provides CRC32 hash computation using the built-in .NET hashing library.
/// </summary>
internal static class Crc32
{
    public static uint Compute(ReadOnlySpan<byte> data)
        => System.IO.Hashing.Crc32.HashToUInt32(data);
}