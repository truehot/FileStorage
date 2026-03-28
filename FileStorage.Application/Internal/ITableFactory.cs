using FileStorage.Abstractions;
using FileStorage.Infrastructure;

namespace FileStorage.Application.Internal;

/// <summary>
/// Creates <see cref="ITable"/> instances. Abstracted for testability.
/// </summary>
internal interface ITableFactory
{
    ITable Create(string name, IStorageEngine engine);
}