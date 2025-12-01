# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.
# See http://www.apache.org/licenses/LICENSE-2.0 for details.

from typing import Any, Dict, List, Literal, Optional, Set, Union, override

import pyarrow as pa
from datus.schemas.node_models import ExecuteSQLResult
from datus.tools.db_tools.base import BaseSqlConnector
from datus.tools.db_tools.config import ConnectionConfig
from datus.utils.constants import DBType
from datus.utils.exceptions import DatusException, ErrorCode
from datus.utils.loggings import get_logger
from datus.utils.sql_utils import parse_context_switch
from pandas import DataFrame
from pyhive import hive
from pyhive import exc as pyhive_exc

from .config import SparkThriftConfig

logger = get_logger(__name__)


def _handle_spark_exception(e: Exception, sql: str = "") -> DatusException:
    """Handle Spark/PyHive exceptions and map to appropriate Datus ErrorCode."""

    # Syntax errors
    if isinstance(e, pyhive_exc.ProgrammingError):
        error_msg = str(e).lower()
        if "parseexception" in error_msg or "syntax" in error_msg:
            return DatusException(
                ErrorCode.DB_EXECUTION_SYNTAX_ERROR,
                message_args={"sql": sql, "error_message": str(e)}
            )
        return DatusException(
            ErrorCode.DB_EXECUTION_ERROR,
            message_args={"sql": sql, "error_message": str(e)}
        )

    # Connection and operational errors
    elif isinstance(e, pyhive_exc.OperationalError):
        error_msg = str(e).lower()
        if "connection" in error_msg or "timeout" in error_msg:
            return DatusException(
                ErrorCode.DB_CONNECTION_FAILED,
                message_args={"error_message": str(e)}
            )
        return DatusException(
            ErrorCode.DB_EXECUTION_ERROR,
            message_args={"sql": sql, "error_message": str(e)}
        )

    # Interface errors
    elif isinstance(e, pyhive_exc.InterfaceError):
        return DatusException(
            ErrorCode.DB_CONNECTION_FAILED,
            message_args={"error_message": str(e)}
        )

    # Database errors
    elif isinstance(e, pyhive_exc.DatabaseError):
        return DatusException(
            ErrorCode.DB_EXECUTION_ERROR,
            message_args={"sql": sql, "error_message": str(e)}
        )

    # Data errors
    elif isinstance(e, pyhive_exc.DataError):
        return DatusException(
            ErrorCode.DB_EXECUTION_ERROR,
            message_args={"sql": sql, "error_message": str(e)}
        )

    # Other errors
    else:
        return DatusException(
            ErrorCode.DB_FAILED,
            message_args={"error_message": str(e)}
        )


class SparkThriftConnector(BaseSqlConnector):
    """
    Connector for Spark Thrift Server using PyHive.

    Supports Spark 3.x without Unity Catalog (database.table naming).
    In Spark, database and schema are synonymous concepts.
    """

    def __init__(self, config: Union[SparkThriftConfig, dict]):
        """
        Initialize Spark Thrift Server connector.

        Args:
            config: SparkThriftConfig object or dict with configuration
        """
        # Handle config object or dict
        if isinstance(config, dict):
            config = SparkThriftConfig(**config)
        elif not isinstance(config, SparkThriftConfig):
            raise TypeError(f"config must be SparkThriftConfig or dict, got {type(config)}")

        self.spark_config = config

        conn_config = ConnectionConfig(timeout_seconds=config.timeout_seconds)
        super().__init__(config=conn_config, dialect=DBType.HIVE)

        self._create_connection(config)

        self.database_name = config.database or ""

    def _create_connection(self, config: SparkThriftConfig, database: Optional[str] = None):
        """
        Create PyHive connection.

        Args:
            config: Spark configuration
            database: Database name to use. If None, uses the original config.database.
                     Pass empty string to not set a default database.
        """
        self.connection = hive.Connection(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            database=database if database is not None else config.database,
            auth=config.auth,
        )

    def test_connection(self) -> Dict[str, Any]:
        """Test the database connection."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchall()
            return {
                "success": True,
                "message": "Connection successful",
            }
        except Exception as e:
            raise _handle_spark_exception(e, "SELECT 1")

    def close(self):
        """Close the database connection."""
        if self.connection:
            try:
                self.connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {str(e)}")

    def get_type(self) -> str:
        """Return the database type."""
        return DBType.HIVE

    def _sys_databases(self) -> Set[str]:
        """Return set of system databases to filter out."""
        return {
            "default",           # Spark default database
            "global_temp",       # Global temporary views database
        }

    def _get_cursor_without_database(self):
        """
        Create a temporary cursor without default database for metadata queries.

        Returns:
            Cursor object without a specific database connection.
        """
        try:
            # Create temporary connection without database parameter
            conn = hive.Connection(
                host=self.spark_config.host,
                port=self.spark_config.port,
                username=self.spark_config.username,
                password=self.spark_config.password,
                database=None,  # Don't specify default database
                auth=self.spark_config.auth,
            )
            return conn.cursor()
        except Exception as e:
            # Fallback to regular connection if failed
            logger.warning(f"Failed to create connection without database: {e}, using default connection")
            return self.connection.cursor()

    @override
    def execute_query(
        self, sql: str, result_format: Literal["csv", "arrow", "pandas", "list"] = "csv"
    ) -> ExecuteSQLResult:
        """Execute SELECT query."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                columns = [col[0] for col in cursor.description] if cursor.description else []

                # Convert to requested format
                if result_format == "arrow":
                    # Build Arrow Table
                    if rows and columns:
                        data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
                        table = pa.table(data)
                    else:
                        table = pa.table({})
                    result = table
                    row_count = len(rows)

                elif result_format == "pandas":
                    df = DataFrame(rows, columns=columns)
                    result = df
                    row_count = len(df)

                elif result_format == "csv":
                    df = DataFrame(rows, columns=columns)
                    result = df.to_csv(index=False)
                    row_count = len(df)

                else:  # list
                    result = [dict(zip(columns, row)) for row in rows]
                    row_count = len(rows)

                return ExecuteSQLResult(
                    success=True,
                    sql_query=sql,
                    sql_return=result,
                    row_count=row_count,
                    result_format=result_format,
                )
        except Exception as e:
            ex = _handle_spark_exception(e, sql)
            return ExecuteSQLResult(
                success=False,
                sql_query=sql,
                error=str(ex),
            )

    @override
    def execute_insert(self, sql: str) -> ExecuteSQLResult:
        """Execute INSERT statement."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                # Spark may not return rowcount for all operations
                rowcount = getattr(cursor, 'rowcount', 0)

                return ExecuteSQLResult(
                    success=True,
                    sql_query=sql,
                    sql_return=str(rowcount),
                    row_count=rowcount,
                )
        except Exception as e:
            ex = _handle_spark_exception(e, sql)
            return ExecuteSQLResult(
                success=False,
                sql_query=sql,
                error=str(ex),
            )

    @override
    def execute_update(self, sql: str) -> ExecuteSQLResult:
        """Execute UPDATE statement."""
        return self._execute_dml(sql)

    @override
    def execute_delete(self, sql: str) -> ExecuteSQLResult:
        """Execute DELETE statement."""
        return self._execute_dml(sql)

    @override
    def execute_ddl(self, sql: str) -> ExecuteSQLResult:
        """Execute DDL statement (CREATE, ALTER, DROP, etc.)."""
        return self._execute_dml(sql)

    def _execute_dml(self, sql: str) -> ExecuteSQLResult:
        """Execute DML/DDL statements."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                rowcount = getattr(cursor, 'rowcount', 0)

                return ExecuteSQLResult(
                    success=True,
                    sql_query=sql,
                    sql_return=str(rowcount),
                    row_count=rowcount,
                )
        except Exception as e:
            ex = _handle_spark_exception(e, sql)
            return ExecuteSQLResult(
                success=False,
                sql_query=sql,
                error=str(ex),
            )

    @override
    def execute_content_set(self, sql: str) -> ExecuteSQLResult:
        """Execute USE/SET commands."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)

            # Update internal state
            context = parse_context_switch(sql=sql, dialect=self.dialect)
            if context:
                if database_name := context.get("database_name"):
                    self.database_name = database_name

            return ExecuteSQLResult(
                success=True,
                sql_query=sql,
                sql_return="Successful",
                row_count=0,
            )
        except Exception as e:
            ex = _handle_spark_exception(e, sql)
            return ExecuteSQLResult(
                success=False,
                sql_query=sql,
                error=str(ex),
            )

    def execute_csv(self, sql: str) -> ExecuteSQLResult:
        """Execute query and return CSV format."""
        return self.execute_query(sql, result_format="csv")

    def execute_pandas(self, sql: str) -> ExecuteSQLResult:
        """Execute query and return pandas DataFrame."""
        return self.execute_query(sql, result_format="pandas")

    def execute_queries(self, queries: List[str]) -> List[ExecuteSQLResult]:
        """Execute multiple queries."""
        return [self.execute_query(sql) for sql in queries]

    @override
    def get_databases(self, catalog_name: str = "", include_sys: bool = False) -> List[str]:
        """Get list of databases."""
        # Only return the configured database
        if self.database_name:
            return [self.database_name]
        # If no database configured, query all databases
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SHOW DATABASES")
                rows = cursor.fetchall()
                # Assuming first column is database name
                databases = [row[0] for row in rows]

                if not include_sys:
                    databases = [db for db in databases if db not in self._sys_databases()]

                return databases
        except Exception as e:
            raise _handle_spark_exception(e, "SHOW DATABASES")

    @override
    def get_tables(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        """Get list of table names."""
        database_name = database_name or self.database_name
        try:
            with self.connection.cursor() as cursor:
                if database_name:
                    sql = f"SHOW TABLES IN {database_name}"
                else:
                    sql = "SHOW TABLES"

                cursor.execute(sql)
                rows = cursor.fetchall()

                # SHOW TABLES returns: database, tableName, isTemporary
                # Extract table names (second column)
                if rows and len(rows[0]) >= 2:
                    tables = [row[1] for row in rows]
                else:
                    # Fallback: assume first column is table name
                    tables = [row[0] for row in rows]

                return tables
        except Exception as e:
            raise _handle_spark_exception(e, sql if 'sql' in locals() else "SHOW TABLES")

    def get_views(self, catalog_name: str = "", database_name: str = "", schema_name: str = "") -> List[str]:
        """Get list of view names."""
        database_name = database_name or self.database_name
        try:
            with self.connection.cursor() as cursor:
                if database_name:
                    sql = f"SHOW VIEWS IN {database_name}"
                else:
                    sql = "SHOW VIEWS"

                cursor.execute(sql)
                rows = cursor.fetchall()

                # Extract view names
                if rows and len(rows[0]) >= 2:
                    views = [row[1] for row in rows]
                else:
                    views = [row[0] for row in rows]

                return views
        except Exception as e:
            logger.warning(f"Failed to get views: {e}")
            return []

    def get_schema(
        self,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        table_name: str = "",
        table_type: str = "table",
    ) -> List[Dict[str, Any]]:
        """Get table schema information."""
        if not table_name:
            return []

        full_name = self.full_name(catalog_name, database_name, schema_name, table_name)

        try:
            with self.connection.cursor() as cursor:
                sql = f"DESCRIBE {full_name}"
                cursor.execute(sql)
                rows = cursor.fetchall()

                result = []
                for i, row in enumerate(rows):
                    # DESCRIBE returns: col_name, data_type, comment
                    result.append({
                        "cid": i,
                        "name": row[0],
                        "type": row[1],
                        "nullable": True,  # Spark doesn't always provide this info
                        "default_value": None,
                        "pk": False,
                        "comment": row[2] if len(row) > 2 else None,
                    })

                return result
        except Exception as e:
            raise _handle_spark_exception(e, f"DESCRIBE {full_name}")

    @override
    def full_name(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", table_name: str = ""
    ) -> str:
        """Build fully qualified table name: `database`.`table`"""
        if database_name:
            return f"`{database_name}`.`{table_name}`"
        return f"`{table_name}`"

    @override
    def get_tables_with_ddl(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = "", tables: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        """Get tables with DDL statements."""
        database_name = database_name or self.database_name
        result = []

        # Get all tables
        try:
            table_list = self.get_tables(catalog_name, database_name, schema_name)

            # Filter tables if specified
            if tables:
                table_list = [t for t in table_list if t in tables]

            # Get DDL for each table
            for table_name in table_list:
                full_name = self.full_name(database_name=database_name, table_name=table_name)

                try:
                    # Get DDL using SHOW CREATE TABLE
                    ddl = self._get_create_statement(full_name, "TABLE")
                except Exception as e:
                    logger.warning(f"Could not get DDL for {full_name}: {e}")
                    ddl = f"-- DDL not available for {table_name}"

                result.append({
                    "identifier": f"{database_name}.{table_name}" if database_name else table_name,
                    "catalog_name": "",
                    "schema_name": "",
                    "database_name": database_name or "",
                    "table_name": table_name,
                    "table_type": "table",
                    "definition": ddl,
                })
        except Exception as e:
            logger.error(f"Failed to get tables with DDL: {e}")
            raise _handle_spark_exception(e, "get_tables_with_ddl")

        return result

    @override
    def get_views_with_ddl(
        self, catalog_name: str = "", database_name: str = "", schema_name: str = ""
    ) -> List[Dict[str, str]]:
        """Get views with DDL statements."""
        database_name = database_name or self.database_name
        result = []

        try:
            view_list = self.get_views(catalog_name, database_name, schema_name)

            for view_name in view_list:
                full_name = self.full_name(database_name=database_name, table_name=view_name)

                try:
                    # Get DDL using SHOW CREATE TABLE (works for views too in Spark)
                    ddl = self._get_create_statement(full_name, "TABLE")
                except Exception as e:
                    logger.warning(f"Could not get DDL for view {full_name}: {e}")
                    ddl = f"-- DDL not available for {view_name}"

                result.append({
                    "identifier": f"{database_name}.{view_name}" if database_name else view_name,
                    "catalog_name": "",
                    "schema_name": "",
                    "database_name": database_name or "",
                    "table_name": view_name,
                    "table_type": "view",
                    "definition": ddl,
                })
        except Exception as e:
            logger.error(f"Failed to get views with DDL: {e}")
            # Return empty list instead of raising exception for views
            return []

        return result

    def _get_create_statement(self, full_name: str, object_type: str = "TABLE") -> str:
        """
        Get CREATE statement for a table or view.

        Args:
            full_name: Fully qualified table/view name
            object_type: Object type (TABLE or VIEW) - not used in Spark as SHOW CREATE TABLE works for both

        Returns:
            DDL statement as string
        """
        try:
            with self.connection.cursor() as cursor:
                # Spark uses SHOW CREATE TABLE for both tables and views
                sql = f"SHOW CREATE TABLE {full_name}"
                cursor.execute(sql)
                result = cursor.fetchall()

                if result and len(result) > 0:
                    # SHOW CREATE TABLE returns a single row with the DDL
                    # The DDL is usually in the first column
                    ddl = result[0][0] if result[0] else ""
                    return ddl

                return f"-- DDL not available for {full_name}"
        except Exception as e:
            logger.warning(f"Failed to get CREATE statement for {full_name}: {e}")
            return f"-- DDL not available for {full_name}"

    @override
    def do_switch_context(self, catalog_name: str = "", database_name: str = "", schema_name: str = ""):
        """
        Switch database context using USE statement.

        Args:
            catalog_name: Catalog name (not used in Spark without Unity Catalog)
            database_name: Database name to switch to
            schema_name: Schema name (same as database in Spark)
        """
        if database_name:
            try:
                with self.connection.cursor() as cursor:
                    cursor.execute(f"USE `{database_name}`")
                    self.database_name = database_name
                    logger.debug(f"Switched to database: {database_name}")
            except Exception as e:
                raise _handle_spark_exception(e, f"USE `{database_name}`")

    @override
    def get_sample_rows(
        self,
        tables: Optional[List[str]] = None,
        top_n: int = 5,
        catalog_name: str = "",
        database_name: str = "",
        schema_name: str = "",
        table_type: str = "table",
    ) -> List[Dict[str, Any]]:
        """
        Get sample rows from tables.

        Args:
            tables: List of table names to sample from. If None, sample from all tables
            top_n: Number of sample rows to retrieve per table
            catalog_name: Catalog name (not used)
            database_name: Database name
            schema_name: Schema name (not used, same as database in Spark)
            table_type: Type of object (table, view)

        Returns:
            List of dictionaries containing sample data for each table
        """
        database_name = database_name or self.database_name
        result = []

        try:
            # Get list of tables to sample
            if tables:
                table_list = tables
            else:
                # Get all tables in the database
                if table_type == "view":
                    table_list = self.get_views(catalog_name, database_name, schema_name)
                else:
                    table_list = self.get_tables(catalog_name, database_name, schema_name)

            # Sample each table
            for table_name in table_list:
                full_name = self.full_name(database_name=database_name, table_name=table_name)

                try:
                    # Query sample rows
                    sql = f"SELECT * FROM {full_name} LIMIT {top_n}"
                    sample_result = self.execute_query(sql, result_format="pandas")

                    if sample_result.success and sample_result.sql_return is not None:
                        df = sample_result.sql_return
                        # Convert DataFrame to CSV string
                        sample_data = df.to_csv(index=False)

                        result.append({
                            "identifier": f"{database_name}.{table_name}" if database_name else table_name,
                            "catalog_name": "",
                            "schema_name": "",
                            "database_name": database_name or "",
                            "table_name": table_name,
                            "table_type": table_type,
                            "sample_data": sample_data,
                        })
                except Exception as e:
                    logger.warning(f"Could not get sample data for {full_name}: {e}")
                    # Continue with next table
                    continue

        except Exception as e:
            logger.error(f"Failed to get sample rows: {e}")
            # Return what we have so far
            pass

        return result
