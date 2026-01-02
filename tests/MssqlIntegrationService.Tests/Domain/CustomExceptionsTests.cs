using FluentAssertions;
using MssqlIntegrationService.Domain.Exceptions;

namespace MssqlIntegrationService.Tests.Domain;

public class CustomExceptionsTests
{
    #region ValidationException Tests

    [Fact]
    public void ValidationException_WithMessage_ShouldSetMessageAndErrors()
    {
        // Arrange
        var message = "Field is required";

        // Act
        var exception = new ValidationException(message);

        // Assert
        exception.Message.Should().Be(message);
        exception.Errors.Should().HaveCount(1);
        exception.Errors[0].Should().Be(message);
    }

    [Fact]
    public void ValidationException_WithMultipleErrors_ShouldSetAllErrors()
    {
        // Arrange
        var errors = new[] { "Name is required", "Email is invalid", "Age must be positive" };

        // Act
        var exception = new ValidationException(errors);

        // Assert
        exception.Message.Should().Be("One or more validation errors occurred.");
        exception.Errors.Should().HaveCount(3);
        exception.Errors.Should().BeEquivalentTo(errors);
    }

    [Fact]
    public void ValidationException_WithInnerException_ShouldSetInnerException()
    {
        // Arrange
        var innerException = new ArgumentException("Inner error");
        var message = "Validation failed";

        // Act
        var exception = new ValidationException(message, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.InnerException.Should().Be(innerException);
        exception.Errors.Should().HaveCount(1);
    }

    #endregion

    #region NotFoundException Tests

    [Fact]
    public void NotFoundException_WithMessage_ShouldSetMessage()
    {
        // Arrange
        var message = "User not found";

        // Act
        var exception = new NotFoundException(message);

        // Assert
        exception.Message.Should().Be(message);
    }

    [Fact]
    public void NotFoundException_WithEntityAndKey_ShouldFormatMessage()
    {
        // Arrange
        var entityName = "User";
        var key = 123;

        // Act
        var exception = new NotFoundException(entityName, key);

        // Assert
        exception.Message.Should().Be("User with key '123' was not found.");
    }

    [Fact]
    public void NotFoundException_WithGuidKey_ShouldFormatMessage()
    {
        // Arrange
        var entityName = "Order";
        var key = Guid.Parse("12345678-1234-1234-1234-123456789abc");

        // Act
        var exception = new NotFoundException(entityName, key);

        // Assert
        exception.Message.Should().Contain("Order");
        exception.Message.Should().Contain("12345678-1234-1234-1234-123456789abc");
    }

    [Fact]
    public void NotFoundException_WithInnerException_ShouldSetInnerException()
    {
        // Arrange
        var innerException = new Exception("Database error");
        var message = "Resource not found";

        // Act
        var exception = new NotFoundException(message, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.InnerException.Should().Be(innerException);
    }

    #endregion

    #region DatabaseException Tests

    [Fact]
    public void DatabaseException_WithMessage_ShouldSetMessage()
    {
        // Arrange
        var message = "Connection failed";

        // Act
        var exception = new DatabaseException(message);

        // Assert
        exception.Message.Should().Be(message);
        exception.ErrorCode.Should().BeNull();
    }

    [Fact]
    public void DatabaseException_WithMessageAndErrorCode_ShouldSetBoth()
    {
        // Arrange
        var message = "Connection timeout";
        var errorCode = "TIMEOUT_001";

        // Act
        var exception = new DatabaseException(message, errorCode);

        // Assert
        exception.Message.Should().Be(message);
        exception.ErrorCode.Should().Be(errorCode);
    }

    [Fact]
    public void DatabaseException_WithInnerException_ShouldSetInnerException()
    {
        // Arrange
        var innerException = new TimeoutException("Timeout");
        var message = "Database operation failed";

        // Act
        var exception = new DatabaseException(message, innerException);

        // Assert
        exception.Message.Should().Be(message);
        exception.InnerException.Should().Be(innerException);
    }

    #endregion
}
