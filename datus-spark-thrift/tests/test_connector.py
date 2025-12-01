# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

import pytest
from datus_spark_thrift import SparkThriftConfig, SparkThriftConnector


@pytest.fixture
def config():
    """Test configuration."""
    return SparkThriftConfig(
        host="localhost",
        port=10000,
        username="test",
        database="default",
    )


def test_config_creation():
    """Test configuration creation."""
    config = SparkThriftConfig(
        host="localhost",
        port=10000,
        username="test",
    )
    assert config.host == "localhost"
    assert config.port == 10000
    assert config.username == "test"
    assert config.auth == "NONE"
    assert config.timeout_seconds == 30


def test_config_from_dict():
    """Test configuration from dictionary."""
    config_dict = {
        "host": "localhost",
        "port": 10000,
        "username": "test",
        "password": "pass",
        "database": "mydb",
    }
    config = SparkThriftConfig(**config_dict)
    assert config.host == "localhost"
    assert config.database == "mydb"


def test_connector_creation(config):
    """Test connector creation."""
    # This will fail without a real Spark server
    # but tests the initialization logic
    try:
        connector = SparkThriftConnector(config)
        assert connector.database_name == "default"
        assert connector.get_type() == "spark"
    except Exception as e:
        # Expected to fail without real server
        assert "connection" in str(e).lower() or "thrift" in str(e).lower()


def test_full_name(config):
    """Test full name generation."""
    connector = SparkThriftConnector(config)

    # With database
    assert connector.full_name(database_name="mydb", table_name="mytable") == "`mydb`.`mytable`"

    # Without database
    assert connector.full_name(table_name="mytable") == "`mytable`"
