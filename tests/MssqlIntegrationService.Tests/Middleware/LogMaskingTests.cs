using System.Reflection;
using FluentAssertions;
using MssqlIntegrationService.API.Middleware;

namespace MssqlIntegrationService.Tests.Middleware;

public class LogMaskingTests
{
    private readonly MethodInfo _maskSensitiveJsonMethod;
    private readonly MethodInfo _maskConnectionStringMethod;

    public LogMaskingTests()
    {
        var middlewareType = typeof(RequestLoggingMiddleware);
        
        _maskSensitiveJsonMethod = middlewareType.GetMethod(
            "MaskSensitiveJson", 
            BindingFlags.NonPublic | BindingFlags.Static)!;
            
        _maskConnectionStringMethod = middlewareType.GetMethod(
            "MaskConnectionString", 
            BindingFlags.NonPublic | BindingFlags.Static)!;
    }

    #region Connection String Masking Tests

    [Theory]
    [InlineData(
        "Server=myserver;Database=MyDB;User Id=sa;Password=MySecret123;",
        "Server=myserver;Database=MyDB;User Id=sa;Password=*** MASKED ***;")]
    [InlineData(
        "Server=localhost;Database=TestDB;Pwd=secret;TrustServerCertificate=true;",
        "Server=localhost;Database=TestDB;Pwd=*** MASKED ***;TrustServerCertificate=true;")]
    [InlineData(
        "Data Source=server;Initial Catalog=db;password=test123",
        "Data Source=server;Initial Catalog=db;password=*** MASKED ***")]
    public void MaskConnectionString_MssqlConnectionStrings_ShouldMaskPassword(string input, string expected)
    {
        // Act
        var result = _maskConnectionStringMethod.Invoke(null, new object[] { input });

        // Assert
        result.Should().Be(expected);
    }

    [Theory]
    [InlineData(
        "mongodb://admin:secretpass@localhost:27017",
        "mongodb://admin:*** MASKED ***@localhost:27017")]
    [InlineData(
        "mongodb://user:simplepass@cluster.mongodb.net/mydb",
        "mongodb://user:*** MASKED ***@cluster.mongodb.net/mydb")]
    [InlineData(
        "mongodb+srv://admin:pass123@cluster0.abc.mongodb.net",
        "mongodb+srv://admin:*** MASKED ***@cluster0.abc.mongodb.net")]
    public void MaskConnectionString_MongoDbConnectionStrings_ShouldMaskPassword(string input, string expected)
    {
        // Act
        var result = _maskConnectionStringMethod.Invoke(null, new object[] { input });

        // Assert
        result.Should().Be(expected);
    }

    [Fact]
    public void MaskConnectionString_NoPassword_ShouldReturnUnchanged()
    {
        // Arrange
        var input = "Server=myserver;Database=MyDB;Integrated Security=true;";

        // Act
        var result = _maskConnectionStringMethod.Invoke(null, new object[] { input });

        // Assert
        result.Should().Be(input);
    }

    #endregion

    #region JSON Masking Tests

    [Fact]
    public void MaskSensitiveJson_WithConnectionString_ShouldMaskPasswordInConnectionString()
    {
        // Arrange
        var json = """
        {
            "connectionString": "Server=myserver;Password=secret123;Database=MyDB;",
            "query": "SELECT * FROM Users"
        }
        """;

        // Act
        var result = (string?)_maskSensitiveJsonMethod.Invoke(null, new object?[] { json });

        // Assert
        result.Should().NotBeNull();
        result.Should().Contain("Server=myserver");
        result.Should().Contain("Database=MyDB");
        result.Should().Contain("*** MASKED ***");
        result.Should().NotContain("secret123");
    }

    [Fact]
    public void MaskSensitiveJson_WithPassword_ShouldMask()
    {
        // Arrange
        var json = """{"password": "mysecretpassword", "username": "admin"}""";

        // Act
        var result = (string?)_maskSensitiveJsonMethod.Invoke(null, new object?[] { json });

        // Assert
        result.Should().NotBeNull();
        result.Should().Contain("*** MASKED ***");
        result.Should().NotContain("mysecretpassword");
        result.Should().Contain("admin");
    }

    [Fact]
    public void MaskSensitiveJson_WithApiKey_ShouldMask()
    {
        // Arrange
        var json = """{"apiKey": "sk-12345678", "endpoint": "https://api.example.com"}""";

        // Act
        var result = (string?)_maskSensitiveJsonMethod.Invoke(null, new object?[] { json });

        // Assert
        result.Should().NotBeNull();
        result.Should().Contain("*** MASKED ***");
        result.Should().NotContain("sk-12345678");
        result.Should().Contain("https://api.example.com");
    }

    [Fact]
    public void MaskSensitiveJson_WithNestedObject_ShouldMaskNested()
    {
        // Arrange
        var json = """
        {
            "source": {
                "connectionString": "mongodb://user:pass@host",
                "database": "mydb"
            },
            "target": {
                "connectionString": "Server=srv;Password=pwd123;",
                "table": "Users"
            }
        }
        """;

        // Act
        var result = (string?)_maskSensitiveJsonMethod.Invoke(null, new object?[] { json });

        // Assert
        result.Should().NotBeNull();
        result.Should().NotContain("pass@");
        result.Should().NotContain("pwd123");
        result.Should().Contain("mydb");
        result.Should().Contain("Users");
    }

    [Fact]
    public void MaskSensitiveJson_WithArray_ShouldMaskInArray()
    {
        // Arrange
        var json = """
        {
            "connections": [
                {"connectionString": "Server=s1;Password=p1;"},
                {"connectionString": "Server=s2;Password=p2;"}
            ]
        }
        """;

        // Act
        var result = (string?)_maskSensitiveJsonMethod.Invoke(null, new object?[] { json });

        // Assert
        result.Should().NotBeNull();
        result.Should().NotContain("p1");
        result.Should().NotContain("p2");
        result.Should().Contain("Server=s1");
        result.Should().Contain("Server=s2");
    }

    [Fact]
    public void MaskSensitiveJson_NullInput_ShouldReturnNull()
    {
        // Act
        var result = _maskSensitiveJsonMethod.Invoke(null, new object?[] { null });

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public void MaskSensitiveJson_EmptyString_ShouldReturnEmpty()
    {
        // Act
        var result = (string?)_maskSensitiveJsonMethod.Invoke(null, new object?[] { "" });

        // Assert
        result.Should().Be("");
    }

    [Fact]
    public void MaskSensitiveJson_NonJsonString_ShouldReturnOriginal()
    {
        // Arrange
        var input = "This is not JSON";

        // Act
        var result = (string?)_maskSensitiveJsonMethod.Invoke(null, new object?[] { input });

        // Assert
        result.Should().Be(input);
    }

    [Fact]
    public void MaskSensitiveJson_InvalidJson_ShouldReturnOriginal()
    {
        // Arrange
        var input = "{invalid json structure";

        // Act
        var result = (string?)_maskSensitiveJsonMethod.Invoke(null, new object?[] { input });

        // Assert
        result.Should().Be(input);
    }

    [Theory]
    [InlineData("token")]
    [InlineData("secret")]
    [InlineData("access_token")]
    [InlineData("client_secret")]
    [InlineData("pwd")]
    public void MaskSensitiveJson_AllSensitiveFields_ShouldBeMasked(string fieldName)
    {
        // Arrange
        var json = $$"""{"{{fieldName}}": "sensitive_value", "safe": "visible"}""";

        // Act
        var result = (string?)_maskSensitiveJsonMethod.Invoke(null, new object?[] { json });

        // Assert
        result.Should().NotBeNull();
        result.Should().NotContain("sensitive_value");
        result.Should().Contain("visible");
    }

    #endregion
}
