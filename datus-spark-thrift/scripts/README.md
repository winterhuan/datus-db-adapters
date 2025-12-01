# Test Scripts

## test_connection.py

Comprehensive validation script for Spark Thrift Server connection.

### Features

1. **Configuration from .env**: Loads connection parameters from `.env` file
2. **Connection Testing**: Validates basic connectivity
3. **Metadata Queries**: Tests database, table, and view listing
4. **Schema Retrieval**: Validates DESCRIBE functionality
5. **Query Execution**: Tests all result formats (CSV, Pandas, Arrow, List)
6. **SQL Command Validation**: Verifies Spark SQL command formats

### Usage

```bash
# 1. Setup environment
cp ../.env.example ../.env
# Edit .env with your credentials

# 2. Install dependencies
pip install -e "..[dev]"

# 3. Run tests
python test_connection.py
```

### Expected Output

The script runs 7 test sections:
1. Testing Connection
2. Testing Database Listing
3. Testing Table Listing
4. Testing View Listing
5. Testing Table Schema Retrieval
6. Testing Query Execution
7. Testing SQL Command Formats

Each section prints detailed results with ✅/❌ indicators.

### Configuration (.env)

Required environment variables:
- `SPARK_HOST` - Spark Thrift Server hostname
- `SPARK_PORT` - Thrift server port (default: 10000)
- `SPARK_USERNAME` - Username for authentication
- `SPARK_PASSWORD` - Password for authentication
- `SPARK_DATABASE` - Default database name
- `SPARK_AUTH` - Authentication method (NONE, CUSTOM, LDAP)
- `SPARK_TIMEOUT` - Connection timeout in seconds

### Troubleshooting

**Connection Failed**
- Check if Spark Thrift Server is running
- Verify host and port are correct
- Ensure firewall allows connections

**Authentication Failed**
- Verify username/password
- Check AUTH method matches server configuration

**Query Failed**
- Check if database exists
- Verify table permissions
- Review Spark server logs
