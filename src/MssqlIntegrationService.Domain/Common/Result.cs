namespace MssqlIntegrationService.Domain.Common;

public class Result<T>
{
    public bool IsSuccess { get; }
    public T? Data { get; }
    public string? ErrorMessage { get; }
    public int? ErrorCode { get; }

    private Result(bool isSuccess, T? data, string? errorMessage, int? errorCode)
    {
        IsSuccess = isSuccess;
        Data = data;
        ErrorMessage = errorMessage;
        ErrorCode = errorCode;
    }

    public static Result<T> Success(T data) => new(true, data, null, null);
    
    public static Result<T> Failure(string errorMessage, int? errorCode = null) 
        => new(false, default, errorMessage, errorCode);
}

public class Result
{
    public bool IsSuccess { get; }
    public string? ErrorMessage { get; }
    public int? ErrorCode { get; }

    private Result(bool isSuccess, string? errorMessage, int? errorCode)
    {
        IsSuccess = isSuccess;
        ErrorMessage = errorMessage;
        ErrorCode = errorCode;
    }

    public static Result Success() => new(true, null, null);
    
    public static Result Failure(string errorMessage, int? errorCode = null) 
        => new(false, errorMessage, errorCode);
}
