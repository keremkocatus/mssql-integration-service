using System.Text.RegularExpressions;

namespace MssqlIntegrationService.Domain.Validation;

/// <summary>
/// SQL Injection koruması için validasyon sınıfı
/// </summary>
public static partial class SqlValidator
{
    // Tehlikeli SQL pattern'leri
    private static readonly string[] DangerousPatterns = new[]
    {
        "--",           // SQL comment
        ";--",          // Statement terminator + comment
        "/*",           // Block comment start
        "*/",           // Block comment end
        "xp_",          // Extended stored procedures
        "sp_",          // System stored procedures (optional - can be removed if needed)
        "EXEC(",        // Execute
        "EXECUTE(",     // Execute
        "DROP ",        // Drop statements
        "TRUNCATE ",    // Truncate statements
        "ALTER ",       // Alter statements
        "CREATE ",      // Create statements (for object creation)
        "GRANT ",       // Permission grants
        "REVOKE ",      // Permission revokes
        "DENY ",        // Permission denies
        "SHUTDOWN",     // Server shutdown
        "WAITFOR",      // Delay/time attacks
        "OPENROWSET",   // External data access
        "OPENDATASOURCE", // External data source
        "BULK INSERT",  // Bulk operations
        "INTO OUTFILE", // File operations
        "LOAD_FILE",    // File loading
    };

    // Sadece harfler, rakamlar, alt çizgi ve nokta içerebilir (schema.table formatı için)
    [GeneratedRegex(@"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$", RegexOptions.Compiled)]
    private static partial Regex ValidIdentifierPattern();

    // Kolon ismi için (sadece harfler, rakamlar, alt çizgi)
    [GeneratedRegex(@"^[a-zA-Z_][a-zA-Z0-9_]*$", RegexOptions.Compiled)]
    private static partial Regex ValidColumnPattern();

    /// <summary>
    /// Tablo adı validasyonu (schema.table veya sadece table)
    /// </summary>
    public static bool IsValidTableName(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            return false;

        if (tableName.Length > 128) // SQL Server identifier limit
            return false;

        return ValidIdentifierPattern().IsMatch(tableName);
    }

    /// <summary>
    /// Kolon adı validasyonu
    /// </summary>
    public static bool IsValidColumnName(string columnName)
    {
        if (string.IsNullOrWhiteSpace(columnName))
            return false;

        if (columnName.Length > 128)
            return false;

        return ValidColumnPattern().IsMatch(columnName);
    }

    /// <summary>
    /// Kolon listesi validasyonu
    /// </summary>
    public static bool AreValidColumnNames(IEnumerable<string> columnNames)
    {
        return columnNames.All(IsValidColumnName);
    }

    /// <summary>
    /// SQL sorgusunda tehlikeli pattern kontrolü
    /// </summary>
    public static (bool IsClean, string? DangerousPattern) CheckForDangerousPatterns(string sql)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return (true, null);

        var upperSql = sql.ToUpperInvariant();

        foreach (var pattern in DangerousPatterns)
        {
            if (upperSql.Contains(pattern.ToUpperInvariant()))
            {
                return (false, pattern);
            }
        }

        return (true, null);
    }

    /// <summary>
    /// Identifier'ı güvenli hale getirir (köşeli parantez ekler)
    /// </summary>
    public static string SafeIdentifier(string identifier)
    {
        if (!IsValidColumnName(identifier) && !IsValidTableName(identifier))
        {
            throw new ArgumentException($"Invalid identifier: {identifier}");
        }

        // Köşeli parantez ile wrap et (SQL Injection'a karşı koruma)
        return $"[{identifier.Replace("]", "]]")}]";
    }

    /// <summary>
    /// Tablo adını güvenli hale getirir (schema.table formatını destekler)
    /// </summary>
    public static string SafeTableName(string tableName)
    {
        if (!IsValidTableName(tableName))
        {
            throw new ArgumentException($"Invalid table name: {tableName}");
        }

        // schema.table formatı
        if (tableName.Contains('.'))
        {
            var parts = tableName.Split('.');
            return $"[{parts[0].Replace("]", "]]")}].[{parts[1].Replace("]", "]]")}]";
        }

        return $"[{tableName.Replace("]", "]]")}]";
    }

    /// <summary>
    /// Validasyon sonucu
    /// </summary>
    public static ValidationResult Validate(string? tableName = null, IEnumerable<string>? columnNames = null, string? query = null)
    {
        var result = new ValidationResult();

        if (tableName != null && !IsValidTableName(tableName))
        {
            result.Errors.Add($"Invalid table name: '{tableName}'. Only letters, numbers, underscores, and dots (for schema.table) are allowed.");
        }

        if (columnNames != null)
        {
            foreach (var col in columnNames)
            {
                if (!IsValidColumnName(col))
                {
                    result.Errors.Add($"Invalid column name: '{col}'. Only letters, numbers, and underscores are allowed.");
                }
            }
        }

        if (query != null)
        {
            var (isClean, dangerousPattern) = CheckForDangerousPatterns(query);
            if (!isClean)
            {
                result.Errors.Add($"Potentially dangerous SQL pattern detected: '{dangerousPattern}'");
            }
        }

        return result;
    }
}

public class ValidationResult
{
    public List<string> Errors { get; } = new();
    public bool IsValid => Errors.Count == 0;

    public override string ToString()
    {
        return IsValid ? "Valid" : string.Join("; ", Errors);
    }
}
