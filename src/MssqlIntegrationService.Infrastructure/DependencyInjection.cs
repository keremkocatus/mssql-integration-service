using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MssqlIntegrationService.Application.Interfaces;
using MssqlIntegrationService.Application.Services;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Infrastructure.Data;
using MssqlIntegrationService.Infrastructure.Logging;
using MssqlIntegrationService.Infrastructure.Services;

namespace MssqlIntegrationService.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration configuration)
    {
        // Default connection (optional - for backward compatibility)
        var connectionString = configuration.GetConnectionString("DefaultConnection");
        if (!string.IsNullOrEmpty(connectionString))
        {
            services.AddSingleton<ISqlConnectionFactory>(_ => new SqlConnectionFactory(connectionString));
            services.AddScoped<IDatabaseService, MssqlDatabaseService>();
            services.AddScoped<IQueryService, QueryService>();
        }

        // Dynamic database services (connection string provided per request)
        services.AddScoped<IDynamicDatabaseService, DynamicDatabaseService>();
        services.AddScoped<IDynamicQueryService, DynamicQueryService>();

        // Data transfer services
        services.AddScoped<IDataTransferService, DataTransferService>();
        services.AddScoped<IDataTransferAppService, DataTransferAppService>();

        // Data sync services (delete-insert pattern)
        services.AddScoped<IDataSyncService, DataSyncService>();
        services.AddScoped<IDataSyncAppService, DataSyncAppService>();

        // MongoDB to MSSQL services
        services.AddScoped<IMongoToMssqlService, MongoToMssqlService>();
        services.AddScoped<IMongoToMssqlAppService, MongoToMssqlAppService>();

        return services;
    }

    public static IServiceCollection AddRequestLogging(this IServiceCollection services, IConfiguration configuration)
    {
        // Bind logging options
        services.Configure<LoggingOptions>(configuration.GetSection(LoggingOptions.SectionName));

        // Register all loggers
        services.AddSingleton<IRequestLogger, ConsoleRequestLogger>();
        services.AddSingleton<IRequestLogger, FileRequestLogger>();
        services.AddSingleton<IRequestLogger, MongoRequestLogger>();

        return services;
    }
}
