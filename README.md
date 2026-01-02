# MSSQL Integration Service

A comprehensive MSSQL integration service built with .NET 9 and Clean Architecture. Provides ETL capabilities including executing SQL queries, data transfer between MSSQL databases, and **MongoDB to MSSQL** data migration via REST API.

## 🚀 Features

- ✅ Execute SELECT queries with parameters
- ✅ Execute INSERT/UPDATE/DELETE commands
- ✅ Execute stored procedures
- ✅ **Dynamic connection strings** (connect to any database per request)
- ✅ **Data transfer between databases** (MSSQL to MSSQL)
- ✅ **MongoDB to MSSQL transfer** (with aggregation pipeline support)
- ✅ **Data sync** (Delete-Insert pattern)
- ✅ **Bulk insert** operations
- ✅ **Request logging** (Console, File, MongoDB)
- ✅ SQL Injection protection
- ✅ Database info & schema discovery
- ✅ Connection testing
- ✅ Health check endpoints
- ✅ Swagger documentation

## 📁 Project Structure

```
src/
├── MssqlIntegrationService.Domain/           # Domain layer (entities, interfaces)
├── MssqlIntegrationService.Application/      # Application layer (DTOs, services)
├── MssqlIntegrationService.Infrastructure/   # Infrastructure layer (SQL implementation)
└── MssqlIntegrationService.API/              # API layer (controllers, middleware)
```

## 🏃‍♂️ Running the Service

```bash
cd src/MssqlIntegrationService.API
dotnet run
```

Swagger UI: `https://localhost:5001/swagger`

## 📡 API Endpoints

### Health Check
| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Basic health check |
| GET | `/api/health/details` | Detailed health info |

### Dynamic Query (Connection per request)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/dynamicquery/test-connection` | Test database connection |
| POST | `/api/dynamicquery/database-info` | Get database schema info |
| POST | `/api/dynamicquery/execute` | Execute SELECT query |
| POST | `/api/dynamicquery/execute-nonquery` | Execute INSERT/UPDATE/DELETE |
| POST | `/api/dynamicquery/execute-sp` | Execute stored procedure |

### Data Transfer
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/datatransfer/transfer` | Transfer data between databases |
| POST | `/api/datatransfer/bulk-insert` | Bulk insert data |

### Data Sync (Delete-Insert Pattern)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/datasync/sync` | Sync data using delete-insert pattern |

### MongoDB to MSSQL Transfer
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/mongotomssql/transfer` | Transfer data from MongoDB to MSSQL |

### Static Query (Uses default connection)
| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/query/execute` | Execute SELECT query |
| POST | `/api/query/execute-nonquery` | Execute INSERT/UPDATE/DELETE |
| POST | `/api/query/execute-sp` | Execute stored procedure |

## 📝 Usage Examples

### Test Connection
```json
POST /api/dynamicquery/test-connection
{
    "connectionString": "Server=myserver;Database=MyDB;User Id=sa;Password=xxx;TrustServerCertificate=true;"
}
```

### Execute Query
```json
POST /api/dynamicquery/execute
{
    "connectionString": "Server=myserver;Database=MyDB;User Id=sa;Password=xxx;TrustServerCertificate=true;",
    "query": "SELECT * FROM Users WHERE IsActive = @isActive",
    "parameters": {
        "isActive": true
    }
}
```

### Transfer Data Between Databases
```json
POST /api/datatransfer/transfer
{
    "source": {
        "connectionString": "Server=source-server;Database=SourceDB;User Id=sa;Password=xxx;TrustServerCertificate=true;",
        "query": "SELECT Id, Name, Email FROM Users WHERE CreatedAt > '2025-01-01'"
    },
    "target": {
        "connectionString": "Server=target-server;Database=TargetDB;User Id=sa;Password=xxx;TrustServerCertificate=true;",
        "tableName": "Users"
    },
    "options": {
        "batchSize": 1000,
        "truncateTargetTable": false,
        "createTableIfNotExists": true,
        "useTransaction": true
    }
}
```

### Bulk Insert
```json
POST /api/datatransfer/bulk-insert
{
    "connectionString": "Server=myserver;Database=MyDB;User Id=sa;Password=xxx;TrustServerCertificate=true;",
    "tableName": "Users",
    "data": [
        { "Name": "John Doe", "Email": "john@example.com" },
        { "Name": "Jane Doe", "Email": "jane@example.com" }
    ],
    "options": {
        "batchSize": 1000,
        "useTransaction": true
    }
}
```

### Data Sync (Delete-Insert Pattern) 🔥
Sync data between databases using delete-insert pattern. Steps:
1. Read data from source database
2. Create temp table in target database
3. Insert data into temp table
4. Delete matching rows from target (based on key columns)
5. Insert from temp table to target table
6. Drop temp table

```json
POST /api/datasync/sync
{
    "source": {
        "connectionString": "Server=source-server;Database=SourceDB;User Id=sa;Password=xxx;TrustServerCertificate=true;",
        "query": "SELECT Id, Name, Email, UpdatedAt FROM Users WHERE IsActive = 1"
    },
    "target": {
        "connectionString": "Server=target-server;Database=TargetDB;User Id=sa;Password=xxx;TrustServerCertificate=true;",
        "tableName": "Users",
        "keyColumns": ["Id"]
    },
    "options": {
        "batchSize": 1000,
        "useTransaction": true,
        "deleteAllBeforeInsert": false
    }
}
```

**Options:**
- `keyColumns`: Columns used to match records for deletion (e.g., `["Id"]` or `["CustomerId", "OrderId"]`)
- `deleteAllBeforeInsert`: If `true`, deletes ALL rows from target before insert (full refresh)
- `columnMappings`: Map source columns to different target column names

### MongoDB to MSSQL Transfer 🍃➡️💾
Transfer data from MongoDB collections to MSSQL tables with support for:
- Filter-based queries
- Aggregation pipelines
- Nested document flattening
- Field mapping
- Array handling

```json
POST /api/mongotomssql/transfer
{
    "source": {
        "connectionString": "mongodb://localhost:27017",
        "databaseName": "mydb",
        "collectionName": "users",
        "filter": "{ \"status\": \"active\", \"age\": { \"$gt\": 18 } }"
    },
    "target": {
        "connectionString": "Server=myserver;Database=MyDB;User Id=sa;Password=xxx;TrustServerCertificate=true;",
        "tableName": "dbo.Users"
    },
    "options": {
        "batchSize": 1000,
        "createTableIfNotExists": true,
        "truncateTargetTable": false,
        "flattenNestedDocuments": true,
        "flattenSeparator": "_",
        "arrayHandling": "Serialize",
        "excludeFields": ["_id", "password"]
    }
}
```

**With Aggregation Pipeline:**
```json
{
    "source": {
        "connectionString": "mongodb://localhost:27017",
        "databaseName": "mydb",
        "collectionName": "orders",
        "aggregationPipeline": "[{ \"$match\": { \"status\": \"completed\" } }, { \"$project\": { \"orderId\": 1, \"total\": 1, \"customerName\": \"$customer.name\" } }]"
    },
    "target": {
        "connectionString": "Server=myserver;Database=MyDB;...",
        "tableName": "dbo.CompletedOrders"
    }
}
```

**Options:**
- `flattenNestedDocuments`: Flatten nested objects (e.g., `address.city` → `address_city`)
- `flattenSeparator`: Separator for flattened field names (default: `_`)
- `arrayHandling`: `"Serialize"` (JSON string), `"Skip"`, or `"FirstElement"`
- `includeFields`: Only transfer specified fields
- `excludeFields`: Exclude specified fields
- `fieldMappings`: Map MongoDB fields to MSSQL columns

### Get Database Info
```json
POST /api/dynamicquery/database-info
{
    "connectionString": "Server=myserver;Database=MyDB;User Id=sa;Password=xxx;TrustServerCertificate=true;",
    "includeTables": true,
    "includeColumns": true
}
```

## 🔧 Configuration

### appsettings.json
```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=localhost;Database=MyDB;User Id=sa;Password=xxx;TrustServerCertificate=true;"
  },
  "RequestLogging": {
    "Enabled": true,
    "LogRequestBody": true,
    "LogResponseBody": false,
    "ExcludePaths": ["/api/health", "/swagger"],
    "Console": {
      "Enabled": true,
      "UseColors": true
    },
    "File": {
      "Enabled": false,
      "Directory": "logs",
      "Format": "json",
      "RollingInterval": "Daily"
    },
    "MongoDB": {
      "Enabled": false,
      "ConnectionString": "mongodb://localhost:27017",
      "DatabaseName": "MssqlIntegrationService",
      "CollectionName": "RequestLogs"
    }
  }
}
```

> **Note:** DefaultConnection is optional. You can use dynamic endpoints without configuring a default connection.

### Request Logging Options
| Option | Description | Default |
|--------|-------------|---------|
| `Enabled` | Enable/disable request logging | `true` |
| `LogRequestBody` | Log request body content | `true` |
| `LogResponseBody` | Log response body content | `false` |
| `ExcludePaths` | Paths to exclude from logging | `[]` |
| `Console.Enabled` | Enable console logging | `true` |
| `File.Enabled` | Enable file logging | `false` |
| `MongoDB.Enabled` | Enable MongoDB logging | `false` |

## 🐳 Docker Support

### Build and Run with Docker

```bash
# Build image
docker build -t mssql-integration-service .

# Run container
docker run -d -p 5000:8080 --name mssql-service mssql-integration-service
```

### Docker Compose (with SQL Server)

```bash
# Development (includes local SQL Server)
docker-compose up -d

# Production (service only)
docker-compose -f docker-compose.prod.yml up -d
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ASPNETCORE_ENVIRONMENT` | Environment (Development/Production) | Production |
| `ConnectionStrings__DefaultConnection` | Default DB connection string | - |

### Access Points

- **API**: http://localhost:5000
- **Swagger**: http://localhost:5000/swagger
- **Health**: http://localhost:5000/api/health

### SQL Server Connection (from docker-compose)

```
Server=sqlserver;Database=master;User Id=sa;Password=YourStrong@Passw0rd;TrustServerCertificate=true;
```

## 🧪 Running Tests

```bash
# Run all tests
dotnet test

# Run with coverage
dotnet test --collect:"XPlat Code Coverage"

# Run specific test project
dotnet test tests/MssqlIntegrationService.Tests
```

## 📜 License

MIT
