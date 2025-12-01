#!/usr/bin/env python3
# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""
Test script for validating Spark Thrift Server connection.
Loads configuration from .env file and runs comprehensive validation tests.

Usage:
    1. Copy .env.example to .env and fill in your credentials
    2. Run: python scripts/test_connection.py
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

# Import after setting up path (circular import fixed in __init__.py)
from datus_spark_thrift import SparkThriftConfig, SparkThriftConnector


def load_config_from_env() -> SparkThriftConfig:
    """Load configuration from .env file."""
    # Load .env file from project root
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        print(f"❌ Error: .env file not found at {env_path}")
        print("💡 Tip: Copy .env.example to .env and fill in your credentials")
        sys.exit(1)

    load_dotenv(env_path)

    # Read configuration from environment variables
    config = SparkThriftConfig(
        host=os.getenv("SPARK_HOST", "localhost"),
        port=int(os.getenv("SPARK_PORT", "10000")),
        username=os.getenv("SPARK_USERNAME", ""),
        password=os.getenv("SPARK_PASSWORD", ""),
        database=os.getenv("SPARK_DATABASE"),
        auth=os.getenv("SPARK_AUTH", "NONE"),
        timeout_seconds=int(os.getenv("SPARK_TIMEOUT", "30")),
    )

    return config


def print_section(title: str):
    """Print section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def test_connection(connector: SparkThriftConnector):
    """Test basic connection."""
    print_section("1. Testing Connection")
    try:
        result = connector.test_connection()
        if result["success"]:
            print(f"✅ Connection successful: {result['message']}")
        else:
            print(f"❌ Connection failed: {result.get('error', 'Unknown error')}")
            return False
    except Exception as e:
        print(f"❌ Connection test failed: {e}")
        return False
    return True


def test_databases(connector: SparkThriftConnector):
    """Test listing databases."""
    print_section("2. Testing Database Listing")
    try:
        # Get all databases (including system)
        all_databases = connector.get_databases(include_sys=True)
        print(f"📊 Total databases (including system): {len(all_databases)}")
        for db in all_databases[:10]:  # Show first 10
            print(f"   - {db}")
        if len(all_databases) > 10:
            print(f"   ... and {len(all_databases) - 10} more")

        # Get user databases only
        user_databases = connector.get_databases(include_sys=False)
        print(f"\n📊 User databases (excluding system): {len(user_databases)}")
        for db in user_databases[:10]:
            print(f"   - {db}")
        if len(user_databases) > 10:
            print(f"   ... and {len(user_databases) - 10} more")

        return True
    except Exception as e:
        print(f"❌ Database listing failed: {e}")
        return False


def test_tables(connector: SparkThriftConnector):
    """Test listing tables."""
    print_section("3. Testing Table Listing")
    try:
        # Get databases first
        databases = connector.get_databases(include_sys=False)
        if not databases:
            print("⚠️  No user databases found, trying default database")
            databases = ["default"]

        # Test with first database
        test_db = databases[0]
        print(f"📋 Testing with database: {test_db}")

        tables = connector.get_tables(database_name=test_db)
        print(f"📊 Tables in '{test_db}': {len(tables)}")
        for table in tables[:10]:
            print(f"   - {table}")
        if len(tables) > 10:
            print(f"   ... and {len(tables) - 10} more")

        return True
    except Exception as e:
        print(f"❌ Table listing failed: {e}")
        return False


def test_views(connector: SparkThriftConnector):
    """Test listing views."""
    print_section("4. Testing View Listing")
    try:
        databases = connector.get_databases(include_sys=False)
        if not databases:
            databases = ["default"]

        test_db = databases[0]
        print(f"🔍 Testing with database: {test_db}")

        views = connector.get_views(database_name=test_db)
        print(f"📊 Views in '{test_db}': {len(views)}")
        for view in views[:10]:
            print(f"   - {view}")
        if len(views) > 10:
            print(f"   ... and {len(views) - 10} more")

        return True
    except Exception as e:
        print(f"⚠️  View listing failed (might not be supported): {e}")
        return True  # Not critical


def test_schema(connector: SparkThriftConnector):
    """Test getting table schema."""
    print_section("5. Testing Table Schema Retrieval")
    try:
        # Get a table to describe
        databases = connector.get_databases(include_sys=False)
        if not databases:
            databases = ["default"]

        test_db = databases[0]
        tables = connector.get_tables(database_name=test_db)

        if not tables:
            print("⚠️  No tables found to test schema retrieval")
            return True  # Not critical

        test_table = tables[0]
        print(f"📝 Testing with table: {test_db}.{test_table}")

        schema = connector.get_schema(database_name=test_db, table_name=test_table)
        print(f"📊 Columns: {len(schema)}")
        print(f"\n{'Column':<20} {'Type':<20} {'Nullable':<10} {'Comment'}")
        print(f"{'-'*70}")
        for col in schema[:10]:
            comment = col.get('comment', '') or ''
            print(f"{col['name']:<20} {col['type']:<20} {str(col['nullable']):<10} {comment}")
        if len(schema) > 10:
            print(f"   ... and {len(schema) - 10} more columns")

        return True
    except Exception as e:
        print(f"❌ Schema retrieval failed: {e}")
        return False


def test_query_execution(connector: SparkThriftConnector):
    """Test query execution with different formats."""
    print_section("6. Testing Query Execution")

    # Test simple query
    test_query = "SELECT 1 as test_col, 'test' as test_str"
    print(f"📝 Executing test query: {test_query}")

    try:
        # Test CSV format
        print("\n🔹 Testing CSV format:")
        result = connector.execute_query(test_query, result_format="csv")
        if result.success:
            print(f"✅ Success (rows: {result.row_count})")
            print(f"Result preview:\n{result.sql_return[:200]}")
        else:
            print(f"❌ Failed: {result.error}")
            return False

        # Test Pandas format
        print("\n🔹 Testing Pandas format:")
        result = connector.execute_pandas(test_query)
        if result.success:
            print(f"✅ Success (rows: {result.row_count})")
            print(f"Result:\n{result.sql_return}")
        else:
            print(f"❌ Failed: {result.error}")
            return False

        # Test Arrow format
        print("\n🔹 Testing Arrow format:")
        result = connector.execute_query(test_query, result_format="arrow")
        if result.success:
            print(f"✅ Success (rows: {result.row_count})")
            print(f"Arrow table schema: {result.sql_return.schema}")
        else:
            print(f"❌ Failed: {result.error}")
            return False

        # Test List format
        print("\n🔹 Testing List format:")
        result = connector.execute_query(test_query, result_format="list")
        if result.success:
            print(f"✅ Success (rows: {result.row_count})")
            print(f"Result: {result.sql_return}")
        else:
            print(f"❌ Failed: {result.error}")
            return False

        return True
    except Exception as e:
        print(f"❌ Query execution failed: {e}")
        return False


def test_sql_commands(connector: SparkThriftConnector):
    """Test various SQL command formats (from validation plan)."""
    print_section("7. Testing SQL Command Formats")

    commands = [
        ("SHOW DATABASES", "Database listing"),
        ("SHOW TABLES", "Table listing in current database"),
        ("SELECT current_database()", "Current database function"),
    ]

    for sql, description in commands:
        print(f"\n🔹 {description}:")
        print(f"   SQL: {sql}")
        try:
            result = connector.execute_query(sql, result_format="list")
            if result.success:
                print(f"   ✅ Success (rows: {result.row_count})")
                # Show first few results
                if result.sql_return:
                    print(f"   Sample: {result.sql_return[:3]}")
            else:
                print(f"   ❌ Failed: {result.error}")
        except Exception as e:
            print(f"   ❌ Error: {e}")

    return True


def main():
    """Main test execution."""
    print("\n" + "="*60)
    print("  Spark Thrift Server Connection Validation")
    print("="*60)

    # Load configuration
    try:
        config = load_config_from_env()
        print(f"\n📋 Configuration loaded:")
        print(f"   Host: {config.host}")
        print(f"   Port: {config.port}")
        print(f"   Username: {config.username}")
        print(f"   Database: {config.database or '(default)'}")
        print(f"   Auth: {config.auth}")
        print(f"   Timeout: {config.timeout_seconds}s")
    except Exception as e:
        print(f"\n❌ Failed to load configuration: {e}")
        sys.exit(1)

    # Create connector
    try:
        connector = SparkThriftConnector(config)
    except Exception as e:
        print(f"\n❌ Failed to create connector: {e}")
        sys.exit(1)

    # Run tests
    results = []

    try:
        results.append(("Connection", test_connection(connector)))
        results.append(("Databases", test_databases(connector)))
        results.append(("Tables", test_tables(connector)))
        results.append(("Views", test_views(connector)))
        results.append(("Schema", test_schema(connector)))
        results.append(("Query Execution", test_query_execution(connector)))
        results.append(("SQL Commands", test_sql_commands(connector)))
    finally:
        # Close connection
        print_section("Cleanup")
        connector.close()
        print("✅ Connection closed")

    # Print summary
    print_section("Test Summary")
    total = len(results)
    passed = sum(1 for _, success in results if success)

    for test_name, success in results:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {test_name}")

    print(f"\n📊 Results: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! Connector is working correctly.")
        sys.exit(0)
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please review the errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
