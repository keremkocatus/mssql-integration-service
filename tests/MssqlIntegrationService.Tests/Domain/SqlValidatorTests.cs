using FluentAssertions;
using MssqlIntegrationService.Domain.Validation;

namespace MssqlIntegrationService.Tests.Domain;

public class SqlValidatorTests
{
    #region Table Name Validation Tests

    [Theory]
    [InlineData("Users")]
    [InlineData("dbo.Users")]
    [InlineData("schema_name.table_name")]
    [InlineData("Table123")]
    [InlineData("_table")]
    public void IsValidTableName_ValidNames_ReturnsTrue(string tableName)
    {
        // Act
        var result = SqlValidator.IsValidTableName(tableName);

        // Assert
        result.Should().BeTrue();
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    [InlineData("Users; DROP TABLE Users--")]
    [InlineData("Users' OR '1'='1")]
    [InlineData("123Table")]       // Starts with number
    [InlineData("table name")]     // Contains space
    [InlineData("table-name")]     // Contains dash
    [InlineData("table@name")]     // Contains special char
    public void IsValidTableName_InvalidNames_ReturnsFalse(string? tableName)
    {
        // Act
        var result = SqlValidator.IsValidTableName(tableName!);

        // Assert
        result.Should().BeFalse();
    }

    #endregion

    #region Column Name Validation Tests

    [Theory]
    [InlineData("Id")]
    [InlineData("FirstName")]
    [InlineData("user_id")]
    [InlineData("Column123")]
    [InlineData("_column")]
    public void IsValidColumnName_ValidNames_ReturnsTrue(string columnName)
    {
        // Act
        var result = SqlValidator.IsValidColumnName(columnName);

        // Assert
        result.Should().BeTrue();
    }

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    [InlineData("column; DROP TABLE--")]
    [InlineData("column name")]    // Space
    [InlineData("column-name")]    // Dash
    [InlineData("123column")]      // Starts with number
    [InlineData("col.name")]       // Dot (not allowed in column names)
    public void IsValidColumnName_InvalidNames_ReturnsFalse(string? columnName)
    {
        // Act
        var result = SqlValidator.IsValidColumnName(columnName!);

        // Assert
        result.Should().BeFalse();
    }

    #endregion

    #region Dangerous Pattern Detection Tests

    [Theory]
    [InlineData("SELECT * FROM Users WHERE Id = @id")]
    [InlineData("INSERT INTO Users (Name) VALUES (@name)")]
    [InlineData("UPDATE Users SET Name = @name WHERE Id = @id")]
    [InlineData("DELETE FROM Users WHERE Id = @id")]
    public void CheckForDangerousPatterns_SafeQueries_ReturnsClean(string query)
    {
        // Act
        var (isClean, pattern) = SqlValidator.CheckForDangerousPatterns(query);

        // Assert
        isClean.Should().BeTrue();
        pattern.Should().BeNull();
    }

    [Theory]
    [InlineData("SELECT * FROM Users; DROP TABLE Users--", "--")]
    [InlineData("SELECT * FROM Users /* comment */", "/*")]
    [InlineData("EXEC(SELECT 1)", "EXEC(")]
    [InlineData("SELECT * FROM OPENROWSET(...)", "OPENROWSET")]
    [InlineData("DROP TABLE Users", "DROP ")]
    [InlineData("TRUNCATE TABLE Users", "TRUNCATE ")]
    [InlineData("WAITFOR DELAY '0:0:5'", "WAITFOR")]
    [InlineData("xp_cmdshell 'dir'", "xp_")]
    public void CheckForDangerousPatterns_DangerousQueries_ReturnsDangerousPattern(string query, string expectedPattern)
    {
        // Act
        var (isClean, pattern) = SqlValidator.CheckForDangerousPatterns(query);

        // Assert
        isClean.Should().BeFalse();
        pattern.Should().Be(expectedPattern);
    }

    #endregion

    #region Safe Identifier Tests

    [Fact]
    public void SafeIdentifier_ValidName_ReturnsBracketedName()
    {
        // Act
        var result = SqlValidator.SafeIdentifier("UserName");

        // Assert
        result.Should().Be("[UserName]");
    }

    [Fact]
    public void SafeIdentifier_NameWithBracket_EscapesBracket()
    {
        // Act - this should throw because ] is not valid in identifier
        var act = () => SqlValidator.SafeIdentifier("User]Name");

        // Assert
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void SafeTableName_SimpleTable_ReturnsBracketedName()
    {
        // Act
        var result = SqlValidator.SafeTableName("Users");

        // Assert
        result.Should().Be("[Users]");
    }

    [Fact]
    public void SafeTableName_SchemaTable_ReturnsBracketedSchemaAndTable()
    {
        // Act
        var result = SqlValidator.SafeTableName("dbo.Users");

        // Assert
        result.Should().Be("[dbo].[Users]");
    }

    [Fact]
    public void SafeTableName_InvalidName_ThrowsArgumentException()
    {
        // Act
        var act = () => SqlValidator.SafeTableName("Users; DROP TABLE--");

        // Assert
        act.Should().Throw<ArgumentException>();
    }

    #endregion

    #region Validate Method Tests

    [Fact]
    public void Validate_ValidInputs_ReturnsValidResult()
    {
        // Act
        var result = SqlValidator.Validate(
            tableName: "dbo.Users",
            columnNames: new[] { "Id", "Name", "Email" }
        );

        // Assert
        result.IsValid.Should().BeTrue();
        result.Errors.Should().BeEmpty();
    }

    [Fact]
    public void Validate_InvalidTableName_ReturnsError()
    {
        // Act
        var result = SqlValidator.Validate(tableName: "Users; DROP TABLE--");

        // Assert
        result.IsValid.Should().BeFalse();
        result.Errors.Should().ContainSingle().Which.Should().Contain("Invalid table name");
    }

    [Fact]
    public void Validate_InvalidColumnNames_ReturnsErrors()
    {
        // Act
        var result = SqlValidator.Validate(columnNames: new[] { "valid_col", "invalid column" });

        // Assert
        result.IsValid.Should().BeFalse();
        result.Errors.Should().ContainSingle().Which.Should().Contain("Invalid column name");
    }

    [Fact]
    public void Validate_MultipleErrors_ReturnsAllErrors()
    {
        // Act
        var result = SqlValidator.Validate(
            tableName: "invalid table",
            columnNames: new[] { "invalid col" }
        );

        // Assert
        result.IsValid.Should().BeFalse();
        result.Errors.Should().HaveCount(2);
    }

    #endregion
}
