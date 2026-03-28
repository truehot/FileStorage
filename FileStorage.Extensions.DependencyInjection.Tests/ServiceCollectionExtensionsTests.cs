using FileStorage.Application;
using Microsoft.Extensions.DependencyInjection;

namespace FileStorage.Extensions.DependencyInjection.Tests
{
    public class ServiceCollectionExtensionsTests
    {
        [Fact]
        public void AddFileStorageProvider_InvalidOptions_Throws()
        {
            IServiceCollection services = new ServiceCollection();
            Assert.Throws<ArgumentNullException>(() => services.AddFileStorageProvider((string)null!));
            Assert.Throws<ArgumentNullException>(() => services.AddFileStorageProvider((FileStorageProviderOptions)null!));
        }
    }
}