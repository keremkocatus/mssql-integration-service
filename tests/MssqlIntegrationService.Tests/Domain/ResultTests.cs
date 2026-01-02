using FluentAssertions;
using MssqlIntegrationService.Domain.Common;

namespace MssqlIntegrationService.Tests.Domain;

public class ResultTests
{
    [Fact]
    public void Success_ShouldCreateSuccessResult()
    {
        // Arrange & Act
        var result = Result.Success();

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.ErrorMessage.Should().BeNull();
        result.ErrorCode.Should().BeNull();
    }

    [Fact]
    public void Failure_ShouldCreateFailureResult()
    {
        // Arrange
        var errorMessage = "Test error";
        var errorCode = 500;

        // Act
        var result = Result.Failure(errorMessage, errorCode);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.ErrorMessage.Should().Be(errorMessage);
        result.ErrorCode.Should().Be(errorCode);
    }

    [Fact]
    public void GenericSuccess_ShouldCreateSuccessResultWithData()
    {
        // Arrange
        var data = "Test data";

        // Act
        var result = Result<string>.Success(data);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Data.Should().Be(data);
        result.ErrorMessage.Should().BeNull();
    }

    [Fact]
    public void GenericFailure_ShouldCreateFailureResultWithoutData()
    {
        // Arrange
        var errorMessage = "Test error";

        // Act
        var result = Result<string>.Failure(errorMessage);

        // Assert
        result.IsSuccess.Should().BeFalse();
        result.Data.Should().BeNull();
        result.ErrorMessage.Should().Be(errorMessage);
    }
}
