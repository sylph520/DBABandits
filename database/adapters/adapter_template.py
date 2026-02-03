"""
TEMPLATE: Database Adapter Implementation Guide

This file provides a minimal template for implementing a new database adapter.
Copy this file, implement each method, and register in database/factory.py.

STEP-BY-STEP GUIDE:
====================

1. COPY THIS TEMPLATE
   cp database/adapters/adapter_template.py database/adapters/mydb_adapter.py

2. IMPLEMENT EACH METHOD
   - Replace "pass" with actual database-specific code
   - Handle connection parameters
   - Map database concepts to the interface

3. REGISTER THE ADAPTER
   Edit database/factory.py:
   ADAPTER_REGISTRY = {
       'mssql': MSSQLAdapter,
       'mydb': MyDBAdapter,  # Add your adapter
   }

4. UPDATE CONFIG
   Edit config/db.conf:
   [SYSTEM]
   db_type = mydb
   
   [mydb]
   server = localhost
   database = mydb_database
   username = user
   password = pass

5. TEST
   Run examples/database_abstraction_demo.py with your database
"""

import logging
from typing import Dict, List, Tuple, Any

from database.base import (
    DatabaseInterface, QueryPlanInfo, TableInfo, ColumnInfo, IndexUsage
)


class MyDBAdapter(DatabaseInterface):
    """
    Template for implementing a new database adapter.
    
    Replace 'MyDB' with your database name (e.g., MySQLAdapter, OracleAdapter).
    Implement each method with database-specific logic.
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        super().__init__(connection_params)
        self.schema_name = connection_params.get('schema', 'public')
        self._tables_global: Dict[str, TableInfo] = None
        self._pk_columns_cache: Dict[str, List[str]] = {}
        
    # ==================== CONNECTION ====================
    
    def connect(self) -> Any:
        """
        Establish database connection.
        
        Returns:
            Connection object specific to your database driver
            
        Example for different databases:
        
        # MySQL with mysql-connector-python
        import mysql.connector
        self._connection = mysql.connector.connect(
            host=self.connection_params['server'],
            database=self.connection_params['database'],
            user=self.connection_params['username'],
            password=self.connection_params['password']
        )
        
        # Oracle with cx_Oracle
        import cx_Oracle
        dsn = cx_Oracle.makedsn(
            self.connection_params['server'], 
            1521, 
            self.connection_params['database']
        )
        self._connection = cx_Oracle.connect(
            user=self.connection_params['username'],
            password=self.connection_params['password'],
            dsn=dsn
        )
        
        # SQLite
        import sqlite3
        self._connection = sqlite3.connect(self.connection_params['database'])
        """
        # TODO: Implement with your database driver
        pass
    
    def disconnect(self) -> None:
        """Close database connection."""
        # TODO: Close connection
        pass
    
    # ==================== INDEX OPERATIONS ====================
    
    def create_index(self, 
                     table_name: str, 
                     column_names: Tuple[str, ...], 
                     index_name: str,
                     include_columns: Tuple[str, ...] = ()) -> float:
        """
        Create an index.
        
        Key differences across databases:
        - MSSQL: CREATE INDEX ... INCLUDE (col1, col2)
        - PostgreSQL: No INCLUDE, all columns in key
        - MySQL: Supports covering indexes differently
        - Oracle: Multiple index types (B-tree, Bitmap)
        
        Returns:
            Creation cost (time in seconds or other metric)
        """
        # TODO: Build CREATE INDEX query for your database
        # TODO: Execute and measure time
        # TODO: Return cost
        pass
    
    def drop_index(self, table_name: str, index_name: str) -> None:
        """
        Drop an index.
        
        Syntax differences:
        - MSSQL: DROP INDEX schema.table.index_name
        - PostgreSQL: DROP INDEX IF EXISTS schema.index_name
        - MySQL: DROP INDEX index_name ON table_name
        - Oracle: DROP INDEX schema.index_name
        """
        # TODO: Build DROP INDEX query
        # TODO: Execute
        pass
    
    # ==================== QUERY EXECUTION ====================
    
    def execute_query(self, query: str, clear_cache: bool = True) -> Tuple[float, List[IndexUsage], List[IndexUsage]]:
        """
        Execute query and return statistics.
        
        CHALLENGES:
        1. Cache clearing - varies by database:
           - MSSQL: DBCC DROPCLEANBUFFERS
           - PostgreSQL: No direct equivalent (restart connection or use pg_buffercache)
           - MySQL: No direct equivalent
           
        2. Index usage tracking:
           - MSSQL: Query plan XML shows index seeks/scans
           - PostgreSQL: EXPLAIN (ANALYZE, BUFFERS) shows index usage
           - MySQL: EXPLAIN FORMAT=JSON or performance_schema
           
        Returns:
            (execution_time, non_clustered_indexes_used, clustered_indexes_used)
        """
        # TODO: Clear cache if possible
        # TODO: Execute query with timing
        # TODO: Get index usage from query plan or system tables
        # TODO: Return results
        pass
    
    def get_query_plan(self, query: str) -> QueryPlanInfo:
        """
        Get estimated query plan without execution.
        
        Methods by database:
        - MSSQL: SET SHOWPLAN_XML ON
        - PostgreSQL: EXPLAIN (FORMAT JSON)
        - MySQL: EXPLAIN FORMAT=JSON
        - Oracle: EXPLAIN PLAN FOR + DBMS_XPLAN
        
        Returns:
            QueryPlanInfo with cost estimates
        """
        # TODO: Get query plan from your database
        # TODO: Parse plan to extract costs
        # TODO: Return QueryPlanInfo
        pass
    
    # ==================== METADATA QUERIES ====================
    
    def get_all_columns(self) -> Tuple[Dict[str, List[str]], int]:
        """
        Get all columns.
        
        Standard SQL approach uses information_schema.columns:
        SELECT table_name, column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'your_schema'
        
        Some databases have their own system catalogs:
        - MSSQL: sys.columns, sys.tables
        - PostgreSQL: pg_attribute, pg_class
        - Oracle: all_tab_columns
        """
        # TODO: Query for all columns
        # TODO: Return {table: [columns]}, count
        pass
    
    def get_tables(self) -> Dict[str, TableInfo]:
        """Get all table metadata."""
        # TODO: Query for all tables
        # TODO: For each table, get row count, PK, columns
        # TODO: Return {table_name: TableInfo}
        pass
    
    def _get_columns(self, table_name: str) -> Dict[str, ColumnInfo]:
        """Helper: Get column info for one table."""
        # TODO: Query column metadata (name, type, size)
        # TODO: Return {column_name: ColumnInfo}
        pass
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get row count.
        
        Options:
        1. Exact: SELECT COUNT(*) FROM table (slow for large tables)
        2. Estimate from system tables (faster)
           - PostgreSQL: pg_class.reltuples
           - MSSQL: sys.dm_db_partition_stats
           - MySQL: information_schema.table_statistics (if available)
        """
        # TODO: Implement row count query
        pass
    
    def get_primary_key(self, table_name: str) -> List[str]:
        """
        Get primary key columns.
        
        Standard SQL:
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE constraint_name = (
            SELECT constraint_name
            FROM information_schema.table_constraints
            WHERE table_name = 'your_table'
            AND constraint_type = 'PRIMARY KEY'
        )
        ORDER BY ordinal_position
        """
        # TODO: Query for PK columns
        # TODO: Return ordered list
        pass
    
    # ==================== INDEX SIZE ESTIMATION ====================
    
    def estimate_index_size(self, 
                           table_name: str, 
                           column_names: Tuple[str, ...],
                           include_columns: Tuple[str, ...] = ()) -> float:
        """
        Estimate index size in MB.
        
        FORMULA:
        size = row_count * (header + pk_size + key_size + include_size) * overhead_factor
        
        Where:
        - header: Database-specific index overhead (e.g., 6-24 bytes)
        - pk_size: Sum of primary key column sizes
        - key_size: Sum of index key column sizes  
        - include_size: Sum of included column sizes
        - overhead_factor: Account for B-tree fragmentation (1.2-1.5)
        
        Returns:
            Estimated size in MB
        """
        # TODO: Get table row count
        # TODO: Calculate column sizes (use _get_column_storage_size)
        # TODO: Apply formula
        # TODO: Return size in MB
        pass
    
    def _get_column_storage_size(self, table_name: str, column_name: str) -> int:
        """
        Estimate storage size for a column in bytes.
        
        Map data types to sizes:
        - INTEGER: 4 bytes
        - BIGINT: 8 bytes
        - VARCHAR(n): avg n bytes
        - TEXT: variable (use average or max)
        - etc.
        """
        # TODO: Get column data type
        # TODO: Return estimated size in bytes
        pass
    
    def get_current_pds_size(self) -> float:
        """
        Get total size of all indexes.
        
        Queries:
        - MSSQL: sys.dm_db_partition_stats
        - PostgreSQL: pg_relation_size() or pg_indexes
        - MySQL: information_schema.statistics + show table status
        - Oracle: dba_segments
        
        Returns:
            Size in MB
        """
        # TODO: Query for total index size
        # TODO: Return size in MB
        pass
    
    def get_database_size(self) -> float:
        """Get total database size in MB."""
        # TODO: Query database size
        # TODO: Return size in MB
        pass
    
    # ==================== ANALYSIS ====================
    
    def get_selectivity(self, query: str, predicates: Dict[str, List[str]]) -> Dict[str, float]:
        """
        Calculate predicate selectivity.
        
        APPROACHES:
        1. Use query plan estimates (EXPLAIN/SHOWPLAN)
        2. Query statistics from system tables
        3. Analyze histograms if available
        
        Returns:
            {table_name: selectivity_ratio (0.0-1.0)}
        """
        # TODO: Get query plan with row estimates
        # TODO: Calculate selectivity = estimated_rows / total_rows
        # TODO: Return selectivity per table
        pass
    
    def remove_all_non_clustered_indexes(self) -> None:
        """
        Remove all non-clustered indexes.
        
        Keep primary keys and unique constraints!
        """
        # TODO: Query all non-PK indexes
        # TODO: Drop each one
        pass
    
    def restart_server(self) -> None:
        """
        Restart database server.
        
        WARNING: Usually requires elevated privileges.
        Consider handling at infrastructure level instead.
        """
        # TODO: Implement if needed, or log warning
        logging.warning("Server restart not implemented for this adapter")


# ==================== COMMON PATTERNS ====================

"""
PATTERN 1: Connection Pooling
-----------------------------
For production use, consider connection pooling:

# PostgreSQL with psycopg2
from psycopg2 import pool
self._pool = psycopg2.pool.SimpleConnectionPool(1, 10, **conn_params)
conn = self._pool.getconn()
# ... use conn ...
self._pool.putconn(conn)

# MySQL with mysql-connector-python
from mysql.connector import pooling
self._pool = mysql.connector.pooling.MySQLConnectionPool(
    pool_name="mypool",
    pool_size=5,
    **conn_params
)
conn = self._pool.get_connection()


PATTERN 2: Query Parameterization
---------------------------------
Always use parameterized queries to prevent SQL injection:

# Good (parameterized):
cursor.execute("SELECT * FROM table WHERE id = %s", (id_value,))

# Bad (string formatting):
cursor.execute(f"SELECT * FROM table WHERE id = {id_value}")


PATTERN 3: Error Handling
--------------------------
Wrap database operations with proper error handling:

try:
    cursor.execute(query)
    result = cursor.fetchall()
except DatabaseSpecificError as e:
    logging.error(f"Database error: {e}")
    raise
except Exception as e:
    logging.error(f"Unexpected error: {e}")
    raise


PATTERN 4: Caching
------------------
Cache metadata that doesn't change often:

if table_name in self._table_cache:
    return self._table_cache[table_name]
    
# Fetch from database
result = self._fetch_table_info(table_name)
self._table_cache[table_name] = result
return result


PATTERN 5: Logging
------------------
Log important operations for debugging:

logging.info(f"Created index {index_name} on {table_name}")
logging.debug(f"Query plan: {plan}")
logging.warning(f"Slow query detected: {query}")
logging.error(f"Failed to execute: {query}, error: {e}")
"""
