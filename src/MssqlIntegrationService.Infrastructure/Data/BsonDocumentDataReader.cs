using System.Data.Common;
using MongoDB.Bson;

namespace MssqlIntegrationService.Infrastructure.Data;

/// <summary>
/// A memory-efficient DbDataReader implementation that streams BsonDocuments from MongoDB.
/// Instead of loading all documents into memory, this processes documents one at a time,
/// making it suitable for bulk operations with large MongoDB collections.
/// </summary>
public sealed class BsonDocumentDataReader : DbDataReader
{
    private readonly IAsyncEnumerator<BsonDocument> _enumerator;
    private readonly List<string> _columns;
    private readonly Dictionary<string, int> _columnOrdinals;
    private readonly Dictionary<string, Type> _columnTypes;
    private readonly Func<BsonDocument, Dictionary<string, object?>> _documentConverter;
    private readonly HashSet<string>? _includeFields;
    private readonly HashSet<string>? _excludeFields;
    
    private Dictionary<string, object?>? _currentRow;
    private bool _isClosed;
    private long _rowCount;
    private int _failedCount;

    public BsonDocumentDataReader(
        IAsyncEnumerator<BsonDocument> enumerator,
        List<string> columns,
        Dictionary<string, Type> columnTypes,
        Func<BsonDocument, Dictionary<string, object?>> documentConverter,
        HashSet<string>? includeFields = null,
        HashSet<string>? excludeFields = null)
    {
        _enumerator = enumerator ?? throw new ArgumentNullException(nameof(enumerator));
        _columns = columns ?? throw new ArgumentNullException(nameof(columns));
        _columnTypes = columnTypes ?? throw new ArgumentNullException(nameof(columnTypes));
        _documentConverter = documentConverter ?? throw new ArgumentNullException(nameof(documentConverter));
        _includeFields = includeFields;
        _excludeFields = excludeFields;
        
        _columnOrdinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _columns.Count; i++)
        {
            _columnOrdinals[_columns[i]] = i;
        }
    }

    /// <summary>
    /// Gets the total number of rows successfully read.
    /// </summary>
    public long RowCount => _rowCount;

    /// <summary>
    /// Gets the number of documents that failed to convert.
    /// </summary>
    public int FailedCount => _failedCount;

    public override bool Read()
    {
        // Use sync-over-async for compatibility (SqlBulkCopy uses sync Read internally)
        return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        if (_isClosed) return false;

        while (await _enumerator.MoveNextAsync())
        {
            try
            {
                var doc = _enumerator.Current;
                _currentRow = _documentConverter(doc);
                
                // Apply field filters
                if (_includeFields != null || _excludeFields != null)
                {
                    var filteredRow = new Dictionary<string, object?>();
                    foreach (var kvp in _currentRow)
                    {
                        // Skip _id by default unless explicitly included
                        if (kvp.Key == "_id" && _includeFields?.Contains("_id") != true)
                            continue;

                        if (_includeFields != null && _includeFields.Count > 0 && !_includeFields.Contains(kvp.Key))
                            continue;
                        if (_excludeFields != null && _excludeFields.Contains(kvp.Key))
                            continue;

                        filteredRow[kvp.Key] = kvp.Value;
                    }
                    _currentRow = filteredRow;
                }

                _rowCount++;
                return true;
            }
            catch
            {
                _failedCount++;
                // Continue to next document on conversion failure
            }
        }

        _currentRow = null;
        return false;
    }

    public override int FieldCount => _columns.Count;
    public override bool HasRows => true;
    public override bool IsClosed => _isClosed;
    public override int Depth => 0;
    public override int RecordsAffected => -1;

    public override object this[int ordinal] => GetValue(ordinal);
    public override object this[string name] => GetValue(GetOrdinal(name));

    public override string GetName(int ordinal)
    {
        if (ordinal < 0 || ordinal >= _columns.Count)
            throw new IndexOutOfRangeException($"Column ordinal {ordinal} is out of range");
        return _columns[ordinal];
    }

    public override int GetOrdinal(string name)
    {
        if (_columnOrdinals.TryGetValue(name, out var ordinal))
            return ordinal;
        throw new IndexOutOfRangeException($"Column '{name}' not found");
    }

    public override object GetValue(int ordinal)
    {
        if (_currentRow == null)
            throw new InvalidOperationException("No current row");

        var columnName = GetName(ordinal);
        return _currentRow.TryGetValue(columnName, out var value) ? value ?? DBNull.Value : DBNull.Value;
    }

    public override int GetValues(object[] values)
    {
        var count = Math.Min(values.Length, FieldCount);
        for (int i = 0; i < count; i++)
        {
            values[i] = GetValue(i);
        }
        return count;
    }

    public override bool IsDBNull(int ordinal)
    {
        var value = GetValue(ordinal);
        return value == null || value == DBNull.Value;
    }

    public override Type GetFieldType(int ordinal)
    {
        var columnName = GetName(ordinal);
        return _columnTypes.TryGetValue(columnName, out var type) ? type : typeof(object);
    }

    public override string GetDataTypeName(int ordinal) => GetFieldType(ordinal).Name;

    // Type-specific getters
    public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetValue(ordinal));
    public override byte GetByte(int ordinal) => Convert.ToByte(GetValue(ordinal));
    public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal));
    public override DateTime GetDateTime(int ordinal) => Convert.ToDateTime(GetValue(ordinal));
    public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal));
    public override double GetDouble(int ordinal) => Convert.ToDouble(GetValue(ordinal));
    public override float GetFloat(int ordinal) => Convert.ToSingle(GetValue(ordinal));
    public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);
    public override short GetInt16(int ordinal) => Convert.ToInt16(GetValue(ordinal));
    public override int GetInt32(int ordinal) => Convert.ToInt32(GetValue(ordinal));
    public override long GetInt64(int ordinal) => Convert.ToInt64(GetValue(ordinal));
    public override string GetString(int ordinal) => Convert.ToString(GetValue(ordinal)) ?? string.Empty;

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var value = GetValue(ordinal);
        if (value is not byte[] bytes) return 0;

        if (buffer == null) return bytes.Length;

        var available = bytes.Length - (int)dataOffset;
        var toCopy = Math.Min(available, length);
        Array.Copy(bytes, (int)dataOffset, buffer, bufferOffset, toCopy);
        return toCopy;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var value = GetString(ordinal);
        if (buffer == null) return value.Length;

        var available = value.Length - (int)dataOffset;
        var toCopy = Math.Min(available, length);
        value.CopyTo((int)dataOffset, buffer, bufferOffset, toCopy);
        return toCopy;
    }

    public override bool NextResult() => false;
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);
    public override IEnumerator<object> GetEnumerator() => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_isClosed)
        {
            _enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
            _isClosed = true;
        }
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_isClosed)
        {
            await _enumerator.DisposeAsync();
            _isClosed = true;
        }
        await base.DisposeAsync();
    }
}
