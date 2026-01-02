namespace MssqlIntegrationService.Domain.Exceptions;

/// <summary>
/// Exception thrown when validation fails
/// </summary>
public class ValidationException : Exception
{
    public IReadOnlyList<string> Errors { get; }

    public ValidationException(string message) : base(message)
    {
        Errors = new List<string> { message };
    }

    public ValidationException(IEnumerable<string> errors) : base("One or more validation errors occurred.")
    {
        Errors = errors.ToList().AsReadOnly();
    }

    public ValidationException(string message, Exception innerException) : base(message, innerException)
    {
        Errors = new List<string> { message };
    }
}

/// <summary>
/// Exception thrown when a requested resource is not found
/// </summary>
public class NotFoundException : Exception
{
    public NotFoundException(string message) : base(message)
    {
    }

    public NotFoundException(string entityName, object key) 
        : base($"{entityName} with key '{key}' was not found.")
    {
    }

    public NotFoundException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

/// <summary>
/// Exception thrown when a database operation fails
/// </summary>
public class DatabaseException : Exception
{
    public string? ErrorCode { get; }

    public DatabaseException(string message) : base(message)
    {
    }

    public DatabaseException(string message, string errorCode) : base(message)
    {
        ErrorCode = errorCode;
    }

    public DatabaseException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
