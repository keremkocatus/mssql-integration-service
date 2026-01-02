using MssqlIntegrationService.API.Middleware;
using MssqlIntegrationService.Infrastructure;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new() { Title = "MSSQL Generic Service API", Version = "v1" });
});

// Add Infrastructure services (Database, etc.)
builder.Services.AddInfrastructure(builder.Configuration);

// Add Request Logging services
builder.Services.AddRequestLogging(builder.Configuration);

var app = builder.Build();

// Configure the HTTP request pipeline
app.UseExceptionMiddleware();

// Add Request Logging middleware (before other middleware)
app.UseRequestLogging();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "MSSQL Generic Service API v1"));
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();

app.Run();
