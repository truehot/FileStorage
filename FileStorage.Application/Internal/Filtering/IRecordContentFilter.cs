namespace FileStorage.Application.Internal.Filtering;

/// <summary>
/// Matches UTF-8 encoded record payload against text criteria.
/// </summary>
internal interface IRecordContentFilter
{
    bool IsMatch(ReadOnlySpan<byte> data, string filterValue, RecordFilterOperator filterOperator = RecordFilterOperator.Contains);
}
