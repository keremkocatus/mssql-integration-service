using System.Data.Common;

namespace MssqlIntegrationService.Infrastructure.Data;

/// <summary>
/// A wrapper around DbDataReader that counts rows as they are read.
/// Used to track row counts during streaming without buffering data in memory.
/// This enables memory-efficient data transfer while still tracking metrics.
/// </summary>
public sealed class RowCountingDataReader : DbDataReader
{
    private readonly DbDataReader _innerReader;
    private long _rowCount;

    public RowCountingDataReader(DbDataReader innerReader)
    {
        _innerReader = innerReader ?? throw new ArgumentNullException(nameof(innerReader));
    }

    /// <summary>
    /// Gets the total number of rows read so far.
    /// </summary>
    public long RowCount => _rowCount;

    public override bool Read()
    {
        var result = _innerReader.Read();
        if (result) _rowCount++;
        return result;
    }

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
    {
        var result = await _innerReader.ReadAsync(cancellationToken);
        if (result) _rowCount++;
        return result;
    }

    // Required DbDataReader abstract members - delegate to inner reader
    public override int FieldCount => _innerReader.FieldCount;
    public override object this[int ordinal] => _innerReader[ordinal];
    public override object this[string name] => _innerReader[name];
    public override int RecordsAffected => _innerReader.RecordsAffected;
    public override bool HasRows => _innerReader.HasRows;
    public override bool IsClosed => _innerReader.IsClosed;
    public override int Depth => _innerReader.Depth;

    public override bool GetBoolean(int ordinal) => _innerReader.GetBoolean(ordinal);
    public override byte GetByte(int ordinal) => _innerReader.GetByte(ordinal);
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) 
        => _innerReader.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
    public override char GetChar(int ordinal) => _innerReader.GetChar(ordinal);
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        => _innerReader.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
    public override string GetDataTypeName(int ordinal) => _innerReader.GetDataTypeName(ordinal);
    public override DateTime GetDateTime(int ordinal) => _innerReader.GetDateTime(ordinal);
    public override decimal GetDecimal(int ordinal) => _innerReader.GetDecimal(ordinal);
    public override double GetDouble(int ordinal) => _innerReader.GetDouble(ordinal);
    public override Type GetFieldType(int ordinal) => _innerReader.GetFieldType(ordinal);
    public override float GetFloat(int ordinal) => _innerReader.GetFloat(ordinal);
    public override Guid GetGuid(int ordinal) => _innerReader.GetGuid(ordinal);
    public override short GetInt16(int ordinal) => _innerReader.GetInt16(ordinal);
    public override int GetInt32(int ordinal) => _innerReader.GetInt32(ordinal);
    public override long GetInt64(int ordinal) => _innerReader.GetInt64(ordinal);
    public override string GetName(int ordinal) => _innerReader.GetName(ordinal);
    public override int GetOrdinal(string name) => _innerReader.GetOrdinal(name);
    public override string GetString(int ordinal) => _innerReader.GetString(ordinal);
    public override object GetValue(int ordinal) => _innerReader.GetValue(ordinal);
    public override int GetValues(object[] values) => _innerReader.GetValues(values);
    public override bool IsDBNull(int ordinal) => _innerReader.IsDBNull(ordinal);
    public override bool NextResult() => _innerReader.NextResult();
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => _innerReader.NextResultAsync(cancellationToken);
    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => _innerReader.IsDBNullAsync(ordinal, cancellationToken);
    public override IEnumerator<object> GetEnumerator() => throw new NotSupportedException();
    
    protected override void Dispose(bool disposing)
    {
        // Don't dispose inner reader - it's managed externally
        base.Dispose(disposing);
    }
}
