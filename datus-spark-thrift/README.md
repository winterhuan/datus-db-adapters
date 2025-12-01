# Datus Spark Thrift Adapter

Spark Thrift Server connector for Datus Agent.

## Installation

```bash
pip install datus-spark-thrift
```

## Usage

```python
from datus_spark_thrift import SparkThriftConfig, SparkThriftConnector

# Create configuration
config = SparkThriftConfig(
    host="localhost",
    port=10000,
    username="user",
    database="default",
)

# Create connector
connector = SparkThriftConnector(config)

# Execute query
result = connector.execute_query("SELECT * FROM table LIMIT 10")
print(result.sql_return)

# Get metadata
databases = connector.get_databases()
tables = connector.get_tables(database_name="default")

# Close connection
connector.close()
```

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| host | str | Yes | - | Spark Thrift Server host |
| port | int | No | 10000 | Thrift server port |
| username | str | No | "" | Username for authentication |
| password | str | No | "" | Password for authentication |
| database | str | No | None | Default database name |
| auth | str | No | "NONE" | Authentication: NONE, CUSTOM, LDAP |
| timeout_seconds | int | No | 30 | Connection timeout in seconds |

## Features

- ✅ Basic queries (SELECT, INSERT, UPDATE, DELETE, DDL)
- ✅ Metadata queries (databases, tables, views)
- ✅ Multiple result formats (CSV, Pandas, Arrow, List)
- ✅ Error handling
- ✅ Connection management
- ⏳ Advanced metadata (table DDL, sample rows)
- ⏳ Unity Catalog support

## Requirements

- Python >= 3.12
- Spark 3.x Thrift Server
- datus-agent >= 0.2.3

## Testing

### Connection Validation Script

A comprehensive test script is provided to validate your Spark Thrift Server connection:

```bash
# 1. Install development dependencies
pip install -e ".[dev]"

# 2. Copy and configure .env file
cp .env.example .env
# Edit .env with your Spark server credentials

# 3. Run validation tests
python scripts/test_connection.py
```

The test script will:
- ✅ Test connection to Spark Thrift Server
- ✅ List databases and tables
- ✅ Retrieve table schemas
- ✅ Execute queries in multiple formats (CSV, Pandas, Arrow, List)
- ✅ Validate SQL command formats

### Unit Tests

```bash
# Run unit tests
pytest tests/

# With coverage
pytest --cov=datus_spark_thrift tests/
```

## License

Apache-2.0
