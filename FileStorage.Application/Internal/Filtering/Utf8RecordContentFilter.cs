using System.Text;

namespace FileStorage.Application.Internal.Filtering;

/// <summary>
/// UTF-8 text filter implementation over raw record bytes.
/// </summary>
internal sealed class Utf8RecordContentFilter : IRecordContentFilter
{
    private static readonly UTF8Encoding StrictUtf8 = new(
        encoderShouldEmitUTF8Identifier: false,
        throwOnInvalidBytes: true);

    private readonly StringComparison _comparisonMode;

    public Utf8RecordContentFilter(StringComparison comparisonMode)
    {
        if (comparisonMode is not StringComparison.OrdinalIgnoreCase and not StringComparison.Ordinal)
            throw new ArgumentException(
                "FilterComparisonMode supports only StringComparison.OrdinalIgnoreCase and StringComparison.Ordinal.",
                nameof(comparisonMode));

        _comparisonMode = comparisonMode;
    }

    public bool IsMatch(ReadOnlySpan<byte> data, string filterValue, RecordFilterOperator filterOperator = RecordFilterOperator.Contains)
    {
        string text;
        try
        {
            text = StrictUtf8.GetString(data);
        }
        catch (DecoderFallbackException)
        {
            return false;
        }

        return filterOperator switch
        {
            RecordFilterOperator.Contains => text.Contains(filterValue, _comparisonMode),
            RecordFilterOperator.Equals => string.Equals(text, filterValue, _comparisonMode),
            RecordFilterOperator.LessThan => string.Compare(text, filterValue, _comparisonMode) < 0,
            RecordFilterOperator.GreaterThan => string.Compare(text, filterValue, _comparisonMode) > 0,
            _ => throw new ArgumentOutOfRangeException(nameof(filterOperator), filterOperator, "Unknown filter operator.")
        };
    }
}
