namespace FileStorage.Infrastructure.Core.Hashing;

/// <summary>
/// Provides CRC32 hash computation using the built-in .NET hashing library.
/// </summary>
internal static class Crc32
{
    public static uint Compute(ReadOnlySpan<byte> data)
        => System.IO.Hashing.Crc32.HashToUInt32(data);

    public static uint Compute(
        ReadOnlySpan<byte> part1,
        ReadOnlySpan<byte> part2,
        ReadOnlySpan<byte> part3,
        ReadOnlySpan<byte> part4)
    {
        var crc = new System.IO.Hashing.Crc32();

        crc.Append(part1);
        crc.Append(part2);
        crc.Append(part3);
        crc.Append(part4);

        return crc.GetCurrentHashAsUInt32();
    }
}