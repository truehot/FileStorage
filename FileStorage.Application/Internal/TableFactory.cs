using FileStorage.Abstractions;
using FileStorage.Application.Internal.Filtering;
using FileStorage.Infrastructure;

namespace FileStorage.Application.Internal;

/// <summary>
/// Default table factory used in production.
/// </summary>
internal sealed class TableFactory : ITableFactory
{
    private readonly IRecordContentFilter _recordContentFilter;

    internal TableFactory(IRecordContentFilter recordContentFilter)
    {
        ArgumentNullException.ThrowIfNull(recordContentFilter);
        _recordContentFilter = recordContentFilter;
    }

    public ITable Create(string name, IStorageEngine engine) => new Table(name, engine, _recordContentFilter);
}