using FileStorage.Abstractions.SecondaryIndex;
using FileStorage.Infrastructure.Indexing.SecondaryIndex;

namespace FileStorage.Infrastructure.Core.Operations;

/// <summary>
/// Executes secondary-index operations under a caller-owned lock.
/// </summary>
internal sealed class StorageIndexOperations
{
    private readonly ISecondaryIndexManager _secondaryIndex;

    internal StorageIndexOperations(ISecondaryIndexManager secondaryIndex)
    {
        ArgumentNullException.ThrowIfNull(secondaryIndex);
        _secondaryIndex = secondaryIndex;
    }

    /// <summary>
    /// Drops a secondary index for the specified table field.
    /// </summary>
    public Task DropIndexAsync(string table, string fieldName)
    {
        _secondaryIndex.DropIndex(table, fieldName);
        return Task.CompletedTask;
    }

    /// <summary>
    /// Returns active secondary indexes for the specified table.
    /// </summary>
    public Task<IReadOnlyList<IndexDefinition>> GetIndexesAsync(string table) =>
        Task.FromResult(_secondaryIndex.GetIndexes(table));

    /// <summary>
    /// Looks up record keys using a secondary index.
    /// </summary>
    public Task<List<Guid>?> LookupByIndexAsync(string table, string fieldName, string value)
    {
        if (!_secondaryIndex.HasIndex(table, fieldName))
            return Task.FromResult<List<Guid>?>(null);

        return Task.FromResult<List<Guid>?>(_secondaryIndex.Lookup(table, fieldName, value));
    }

    /// <summary>
    /// Ensures a secondary index exists for the specified table field.
    /// </summary>
    public Task EnsureIndexAsync(string table, string fieldName)
    {
        if (string.IsNullOrEmpty(fieldName))
            throw new ArgumentException("Field name required", nameof(fieldName));

        _secondaryIndex.EnsureIndex(table, fieldName);
        return Task.CompletedTask;
    }
}
