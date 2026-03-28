namespace FileStorage.Application.Internal.Filtering;

/// <summary>
/// Text filter operator for record content matching.
/// Designed for future extension in table query API.
/// </summary>
internal enum RecordFilterOperator
{
    Contains = 0,
    Equals = 1,
    LessThan = 2,
    GreaterThan = 3
}
