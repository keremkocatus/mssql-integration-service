# Build stage
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

# Copy solution and project files
COPY MssqlIntegrationService.sln .
COPY src/MssqlIntegrationService.Domain/MssqlIntegrationService.Domain.csproj src/MssqlIntegrationService.Domain/
COPY src/MssqlIntegrationService.Application/MssqlIntegrationService.Application.csproj src/MssqlIntegrationService.Application/
COPY src/MssqlIntegrationService.Infrastructure/MssqlIntegrationService.Infrastructure.csproj src/MssqlIntegrationService.Infrastructure/
COPY src/MssqlIntegrationService.API/MssqlIntegrationService.API.csproj src/MssqlIntegrationService.API/

# Restore dependencies
RUN dotnet restore

# Copy source code
COPY src/ src/

# Build and publish
WORKDIR /src/src/MssqlIntegrationService.API
RUN dotnet publish -c Release -o /app/publish --no-restore

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app

# Create non-root user for security
RUN adduser --disabled-password --gecos '' appuser

# Copy published app
COPY --from=build /app/publish .

# Set ownership
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose port
EXPOSE 8080

# Health check (using wget as curl is not available in aspnet image)
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/api/health || exit 1

# Set environment variables
ENV ASPNETCORE_URLS=http://+:8080
ENV ASPNETCORE_ENVIRONMENT=Production

# Start application
ENTRYPOINT ["dotnet", "MssqlIntegrationService.API.dll"]
