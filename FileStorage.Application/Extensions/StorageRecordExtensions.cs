using FileStorage.Abstractions;
using System.Text;

namespace FileStorage.Application.Extensions;

/// <summary>
/// Convenience helpers for decoding table payloads represented by <see cref="StorageRecord"/>.
/// </summary>
public static class StorageRecordExtensions
{
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    /// <summary>
    /// Decodes <see cref="StorageRecord.Data"/> as UTF-8 text.
    /// </summary>
    public static string GetDataAsUtf8String(this StorageRecord record)
    {
        ArgumentNullException.ThrowIfNull(record);
        return Encoding.UTF8.GetString(record.Data);
    }

    /// <summary>
    /// Attempts strict UTF-8 decoding for <see cref="StorageRecord.Data"/>.
    /// </summary>
    public static bool TryGetDataAsUtf8String(this StorageRecord record, out string value)
    {
        ArgumentNullException.ThrowIfNull(record);

        try
        {
            value = StrictUtf8.GetString(record.Data);
            return true;
        }
        catch (DecoderFallbackException)
        {
            value = string.Empty;
            return false;
        }
    }
}