using System.Collections;
using System.Data;
using System.Data.Common;

namespace MssqlIntegrationService.Infrastructure.Data;

/// <summary>
/// A memory-efficient DbDataReader implementation that streams data from an IEnumerable source.
/// Instead of loading all data into a DataTable, this reads one item at a time,
/// making it suitable for bulk operations with large datasets.
/// </summary>
/// <typeparam name="T">The type of items in the enumerable (typically IDictionary{string, object?})</typeparam>
public sealed class ObjectDataReader<T> : DbDataReader where T : IDictionary<string, object?>
{
    private readonly IEnumerator<T> _enumerator;
    private readonly List<string> _columns;
    private readonly Dictionary<string, int> _columnOrdinals;
    private T? _current;
    private bool _isClosed;
    private long _rowCount;
    private bool _hasInitialRow; // True if first row was read during initialization

    public ObjectDataReader(IEnumerable<T> data, IEnumerable<string>? columns = null)
    {
        ArgumentNullException.ThrowIfNull(data);
        
        _enumerator = data.GetEnumerator();
        
        // Try to get columns from first item if not provided
        if (columns != null)
        {
            _columns = columns.ToList();
        }
        else
        {
            // Peek at first item to get columns
            if (_enumerator.MoveNext())
            {
                _columns = _enumerator.Current.Keys.ToList();
                _current = _enumerator.Current;
                _rowCount = 1;
                _hasInitialRow = true; // Mark that we pre-loaded first row
            }
            else
            {
                _columns = [];
            }
        }
        
        _columnOrdinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _columns.Count; i++)
        {
            _columnOrdinals[_columns[i]] = i;
        }
    }

    /// <summary>
    /// Gets the total number of rows read so far.
    /// </summary>
    public long RowCount => _rowCount;

    public override bool Read()
    {
        if (_isClosed) return false;
        
        // If we have a pre-loaded initial row, return it on first Read() call
        if (_hasInitialRow)
        {
            _hasInitialRow = false; // Consume the initial row
            return true;
        }
        
        if (_enumerator.MoveNext())
        {
            _current = _enumerator.Current;
            _rowCount++;
            return true;
        }
        
        _current = default;
        return false;
    }

    public override Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(Read());
    }

    public override int FieldCount => _columns.Count;
    public override bool HasRows => true; // Assume there might be rows
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
        if (_current == null)
            throw new InvalidOperationException("No current row");
        
        var columnName = GetName(ordinal);
        return _current.TryGetValue(columnName, out var value) ? value ?? DBNull.Value : DBNull.Value;
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
        if (_current == null) return typeof(object);
        
        var columnName = GetName(ordinal);
        if (_current.TryGetValue(columnName, out var value) && value != null)
        {
            return value.GetType();
        }
        return typeof(object);
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
            _enumerator.Dispose();
            _isClosed = true;
        }
        base.Dispose(disposing);
    }
}
