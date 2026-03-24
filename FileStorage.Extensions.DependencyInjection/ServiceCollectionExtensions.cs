using FileStorage.Application;
using Microsoft.Extensions.DependencyInjection;

namespace FileStorage.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for registering FileStorageProvider in the dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a singleton <see cref="FileStorageProvider"/> for the specified file path.
    /// </summary>
    /// <param name="services">The service collection to add the provider to.</param>
    /// <param name="filePath">The file path for the database storage.</param>
    /// <returns>The updated <see cref="IServiceCollection"/>.</returns>
    /// <exception cref="ArgumentException">Thrown if <paramref name="filePath"/> is null or whitespace.</exception>
    public static IServiceCollection AddFileStorageProvider(this IServiceCollection services, string filePath)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        services.AddSingleton(new FileStorageProvider(filePath));

        return services;
    }
}