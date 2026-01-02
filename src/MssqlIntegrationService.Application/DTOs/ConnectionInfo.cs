namespace MssqlIntegrationService.Application.DTOs;

public class ConnectionInfo
{
    public required string Server { get; set; }
    public required string Database { get; set; }
    public string? UserId { get; set; }
    public string? Password { get; set; }
    public bool IntegratedSecurity { get; set; } = false;
    public bool TrustServerCertificate { get; set; } = true;
    public int ConnectionTimeout { get; set; } = 30;
    public int CommandTimeout { get; set; } = 30;
}
