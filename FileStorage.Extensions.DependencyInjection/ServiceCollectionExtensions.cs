using FileStorage.Abstractions;
using FileStorage.Application;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace FileStorage.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for registering file storage services in the dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a singleton <see cref="IFileStorageProvider"/> backed by <see cref="FileStorageProvider"/> for the specified file path.
    /// </summary>
    public static IServiceCollection AddFileStorageProvider(this IServiceCollection services, string filePath)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        services.AddSingleton<IFileStorageProvider>(sp =>
        {
            var logger = sp.GetService<ILogger<FileStorageProvider>>() ?? NullLogger<FileStorageProvider>.Instance;
            return new FileStorageProvider(filePath, logger);
        });

        return services;
    }

    /// <summary>
    /// Registers a singleton <see cref="IFileStorageProvider"/> backed by <see cref="FileStorageProvider"/> for the specified options.
    /// </summary>
    public static IServiceCollection AddFileStorageProvider(this IServiceCollection services, FileStorageProviderOptions options)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(options);

        var optionsSnapshot = CloneOptions(options);

        services.AddSingleton<IFileStorageProvider>(sp =>
        {
            var logger = sp.GetService<ILogger<FileStorageProvider>>() ?? NullLogger<FileStorageProvider>.Instance;
            return new FileStorageProvider(optionsSnapshot, logger);
        });

        return services;
    }

    /// <summary>
    /// Registers a singleton <see cref="IFileStorageProvider"/> with file path and additional configuration.
    /// </summary>
    public static IServiceCollection AddFileStorageProvider(this IServiceCollection services, string filePath, Action<FileStorageProviderOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new FileStorageProviderOptions { FilePath = filePath };
        configure(options);

        var optionsSnapshot = CloneOptions(options);

        services.AddSingleton<IFileStorageProvider>(sp =>
        {
            var logger = sp.GetService<ILogger<FileStorageProvider>>() ?? NullLogger<FileStorageProvider>.Instance;
            return new FileStorageProvider(optionsSnapshot, logger);
        });

        return services;
    }

    private static FileStorageProviderOptions CloneOptions(FileStorageProviderOptions options) =>
        new()
        {
            FilePath = options.FilePath,
            CheckpointWriteThreshold = options.CheckpointWriteThreshold,
            SecondaryIndexFlushThreshold = options.SecondaryIndexFlushThreshold,
            SecondaryIndexCompactionThreshold = options.SecondaryIndexCompactionThreshold,
            ReadParallelism = options.ReadParallelism,
            FilterComparisonMode = options.FilterComparisonMode,
            DeleteFilesOnStartup = options.DeleteFilesOnStartup,
            DeleteFilesOnDispose = options.DeleteFilesOnDispose
        };
}