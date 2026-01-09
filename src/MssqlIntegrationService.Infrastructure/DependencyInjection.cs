using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;
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
        services.AddScoped<ISchemaService, SchemaService>();
        services.AddScoped<IDataSyncService, DataSyncService>();
        services.AddScoped<IDataSyncAppService, DataSyncAppService>();

        // MongoDB to MSSQL services
        services.AddScoped<IMongoToMssqlService, MongoToMssqlService>();
        services.AddScoped<IMongoToMssqlAppService, MongoToMssqlAppService>();

        return services;
    }

    /// <summary>
    /// Adds background job processing services
    /// </summary>
    public static IServiceCollection AddJobProcessing(this IServiceCollection services, IConfiguration configuration)
    {
        // Get MongoDB settings for job storage
        var jobSettings = configuration.GetSection("JobProcessing");
        var mongoConnectionString = jobSettings["MongoConnectionString"] ?? "mongodb://localhost:27017";
        var mongoDatabaseName = jobSettings["MongoDatabaseName"] ?? "MssqlIntegrationService";
        var collectionName = jobSettings["CollectionName"] ?? "Jobs";

        // Register MongoDB for job storage
        services.AddSingleton<IMongoClient>(_ => new MongoClient(mongoConnectionString));
        services.AddSingleton(sp =>
        {
            var client = sp.GetRequiredService<IMongoClient>();
            return client.GetDatabase(mongoDatabaseName);
        });

        // Register job repository
        services.AddSingleton<IJobRepository>(sp =>
        {
            var database = sp.GetRequiredService<IMongoDatabase>();
            return new MongoJobRepository(database, collectionName);
        });

        // Register background job processor (singleton - hosted service)
        services.AddSingleton<BackgroundJobProcessor>();
        services.AddSingleton<IJobQueueService>(sp => sp.GetRequiredService<BackgroundJobProcessor>());
        services.AddHostedService(sp => sp.GetRequiredService<BackgroundJobProcessor>());

        // Register job app service
        services.AddScoped<IJobAppService, JobAppService>();

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
