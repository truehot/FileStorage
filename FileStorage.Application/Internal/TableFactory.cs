using FileStorage.Abstractions;
using FileStorage.Infrastructure;

namespace FileStorage.Application;

/// <summary>
/// Default table factory used in production.
/// </summary>
internal sealed class TableFactory : ITableFactory
{
    public ITable Create(string name, IStorageEngine engine) => new Table(name, engine);
}