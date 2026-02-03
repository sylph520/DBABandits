"""
Microsoft SQL Server adapter implementing DatabaseInterface.
"""

import logging
from typing import Dict, List, Tuple, Any
import pyodbc

from database.base import (
    DatabaseInterface, QueryPlanInfo, TableInfo, ColumnInfo,
    IndexUsage
)
from database.query_plan import QueryPlan
from database.table import Table
from database.column import Column
import constants


class MSSQLAdapter(DatabaseInterface):
    """
    Microsoft SQL Server adapter for DBA Bandits.
    
    Implements all database operations using pyodbc and SQL Server-specific features
    like hypothetical indexes and query plan XML.
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        super().__init__(connection_params)
        self.schema_name = connection_params.get('schema', 'dbo')
        self._tables_global: Dict[str, Table] = None
        self._pk_columns_cache: Dict[str, List[str]] = {}
        self._table_scan_times = {}
        
    def connect(self) -> Any:
        """Establish connection to SQL Server."""
        driver = self.connection_params.get('driver', 'ODBC Driver 18 for SQL Server')
        server = self.connection_params['server']
        database = self.connection_params['database']
        username = self.connection_params.get('username', 'sa')
        password = self.connection_params['password']
        
        dsn = (f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
               f"UID={username};PWD={password};TrustServerCertificate=Yes;")
        
        self._connection = pyodbc.connect(dsn)
        return self._connection
    
    def disconnect(self) -> None:
        """Close SQL Server connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def create_index(self, 
                     table_name: str, 
                     column_names: Tuple[str, ...], 
                     index_name: str,
                     include_columns: Tuple[str, ...] = ()) -> float:
        """Create a non-clustered index and return creation cost."""
        if include_columns:
            query = (f"CREATE NONCLUSTERED INDEX {index_name} ON {self.schema_name}.{table_name} "
                    f"({', '.join(column_names)}) INCLUDE ({', '.join(include_columns)})")
        else:
            query = (f"CREATE NONCLUSTERED INDEX {index_name} ON {self.schema_name}.{table_name} "
                    f"({', '.join(column_names)})")
        
        cursor = self._connection.cursor()
        cursor.execute("SET STATISTICS XML ON")
        cursor.execute(query)
        stat_xml = cursor.fetchone()[0]
        cursor.execute("SET STATISTICS XML OFF")
        self._connection.commit()
        
        logging.info(f"Added: {index_name}")
        
        # Parse creation cost from query plan
        query_plan = QueryPlan(stat_xml)
        if constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_ELAPSED_TIME:
            return float(query_plan.elapsed_time)
        elif constants.COST_TYPE_CURRENT_CREATION == constants.COST_TYPE_CPU_TIME:
            return float(query_plan.cpu_time)
        else:
            return float(query_plan.est_statement_sub_tree_cost)
    
    def drop_index(self, table_name: str, index_name: str) -> None:
        """Drop an existing index."""
        query = f"DROP INDEX {self.schema_name}.{table_name}.{index_name}"
        cursor = self._connection.cursor()
        cursor.execute(query)
        self._connection.commit()
        logging.info(f"removed: {index_name}")
    
    def execute_query(self, query: str, clear_cache: bool = True) -> Tuple[float, List[IndexUsage], List[IndexUsage]]:
        """Execute query with optional cache clearing and return statistics."""
        try:
            cursor = self._connection.cursor()
            
            if clear_cache:
                cursor.execute("CHECKPOINT;")
                cursor.execute("DBCC DROPCLEANBUFFERS;")
            
            cursor.execute("SET STATISTICS XML ON")
            cursor.execute(query)
            cursor.nextset()
            stat_xml = cursor.fetchone()[0]
            cursor.execute("SET STATISTICS XML OFF")
            
            query_plan = QueryPlan(stat_xml)
            
            # Convert to IndexUsage objects
            nc_usage = []
            for usage in query_plan.non_clustered_index_usage:
                nc_usage.append(IndexUsage(
                    index_name=usage[0],
                    table_name="",  # Will be filled by caller
                    scan_count=1,
                    elapsed_time=usage[1] if len(usage) > 1 else 0,
                    cpu_time=usage[2] if len(usage) > 2 else 0,
                    sub_tree_cost=usage[3] if len(usage) > 3 else 0
                ))
            
            c_usage = []
            for usage in query_plan.clustered_index_usage:
                c_usage.append(IndexUsage(
                    index_name="clustered",
                    table_name=usage[0],
                    scan_count=1,
                    elapsed_time=usage[1] if len(usage) > 1 else 0,
                    cpu_time=usage[2] if len(usage) > 2 else 0,
                    sub_tree_cost=usage[3] if len(usage) > 3 else 0
                ))
            
            if constants.COST_TYPE_CURRENT_EXECUTION == constants.COST_TYPE_ELAPSED_TIME:
                exec_time = float(query_plan.elapsed_time)
            elif constants.COST_TYPE_CURRENT_EXECUTION == constants.COST_TYPE_CPU_TIME:
                exec_time = float(query_plan.cpu_time)
            else:
                exec_time = float(query_plan.est_statement_sub_tree_cost)
            
            return exec_time, nc_usage, c_usage
            
        except Exception as e:
            logging.error(f"Exception when executing query: {query}, error: {e}")
            return 0, [], []
    
    def get_query_plan(self, query: str) -> QueryPlanInfo:
        """Get estimated query plan without execution."""
        cursor = self._connection.cursor()
        cursor.execute("SET SHOWPLAN_XML ON;")
        cursor.execute(query)
        plan_xml = cursor.fetchone()[0]
        cursor.execute("SET SHOWPLAN_XML OFF;")
        
        query_plan = QueryPlan(plan_xml)
        return QueryPlanInfo(
            elapsed_time=float(query_plan.elapsed_time),
            cpu_time=float(query_plan.cpu_time),
            est_statement_sub_tree_cost=float(query_plan.est_statement_sub_tree_cost),
            non_clustered_index_usage=query_plan.non_clustered_index_usage,
            clustered_index_usage=query_plan.clustered_index_usage
        )
    
    def get_all_columns(self) -> Tuple[Dict[str, List[str]], int]:
        """Get all columns in the database."""
        from collections import defaultdict
        
        query = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS;"
        cursor = self._connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        columns = defaultdict(list)
        for result in results:
            columns[result[0]].append(result[1])
        
        return dict(columns), len(results)
    
    def get_tables(self) -> Dict[str, TableInfo]:
        """Get metadata for all tables."""
        if self._tables_global is not None:
            return self._tables_global
        
        tables = {}
        query = """SELECT TABLE_NAME 
                   FROM INFORMATION_SCHEMA.TABLES 
                   WHERE TABLE_TYPE = 'BASE TABLE'"""
        
        cursor = self._connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        for result in results:
            table_name = result[0]
            row_count = self.get_table_row_count(table_name)
            pk_columns = self.get_primary_key(table_name)
            
            table = Table(table_name, row_count, pk_columns)
            table.set_columns(self._get_columns(table_name))
            tables[table_name] = table
        
        self._tables_global = tables
        return tables
    
    def _get_columns(self, table_name: str) -> Dict[str, Column]:
        """Helper to get column metadata for a table."""
        columns = {}
        cursor = self._connection.cursor()
        
        # Get basic column info
        query = f"""SELECT COLUMN_NAME, DATA_TYPE, COL_LENGTH('{table_name}', COLUMN_NAME)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{table_name}'"""
        cursor.execute(query)
        results = cursor.fetchall()
        
        varchar_ids = []
        for result in results:
            col_name = result[0]
            column = Column(table_name, col_name, result[1])
            column.set_max_column_size(int(result[2]))
            
            if result[1] != 'varchar':
                column.set_column_size(int(result[2]))
            else:
                varchar_ids.append(col_name)
            
            columns[col_name] = column
        
        # Get average sizes for varchar columns
        if varchar_ids:
            select_parts = [f"AVG(DL_{col})" for col in varchar_ids]
            inner_parts = [f"DATALENGTH({col}) DL_{col}" for col in varchar_ids]
            
            query = (f"SELECT {', '.join(select_parts)} "
                    f"FROM (SELECT TOP (1000) {', '.join(inner_parts)} FROM {table_name}) T")
            
            cursor.execute(query)
            result_row = cursor.fetchone()
            for i, col_name in enumerate(varchar_ids):
                columns[col_name].set_column_size(result_row[i])
        
        return columns
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        query = f"SELECT COUNT(1) FROM {table_name};"
        cursor = self._connection.cursor()
        cursor.execute(query)
        return cursor.fetchone()[0]
    
    def get_primary_key(self, table_name: str) -> List[str]:
        """Get primary key columns for a table."""
        if table_name in self._pk_columns_cache:
            return self._pk_columns_cache[table_name]
        
        query = f"""SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + 
                          QUOTENAME(CONSTRAINT_NAME)), 'IsPrimaryKey') = 1
                    AND TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = '{self.schema_name}'"""
        
        cursor = self._connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        pk_columns = [result[0] for result in results]
        self._pk_columns_cache[table_name] = pk_columns
        return pk_columns
    
    def estimate_index_size(self, 
                           table_name: str, 
                           column_names: Tuple[str, ...],
                           include_columns: Tuple[str, ...] = ()) -> float:
        """Estimate index size in MB."""
        tables = self.get_tables()
        table = tables[table_name]
        
        header_size = 6
        nullable_buffer = 2
        
        # Get primary key size
        pk_columns = self.get_primary_key(table_name)
        pk_size = self._get_column_data_length(table_name, pk_columns)
        
        # Get key columns size (excluding PK columns)
        non_pk_cols = tuple(set(column_names) - set(pk_columns))
        key_size = self._get_column_data_length(table_name, non_pk_cols)
        
        # Calculate total row size
        row_size = header_size + pk_size + key_size + nullable_buffer
        estimated_size = table.table_row_count * row_size
        estimated_size = estimated_size / float(1024 * 1024)
        
        # Check if exceeds SQL Server limit
        max_length = sum(
            tables[table_name].columns[col].max_column_size or 0 
            for col in column_names
        )
        if max_length > 1700:
            logging.warning(f"Index exceeds 1700 bytes: {column_names}")
            return 99999999
        
        return estimated_size
    
    def _get_column_data_length(self, table_name: str, column_names: List[str]) -> int:
        """Calculate total data length for columns."""
        tables = self.get_tables()
        varchar_count = 0
        total_length = 0
        
        for col_name in column_names:
            column = tables[table_name].columns[col_name]
            if column.column_type == 'varchar':
                varchar_count += 1
            total_length += column.column_size or 0
        
        if varchar_count > 0:
            total_length += 2 + varchar_count * 2  # Variable key overhead
        
        return total_length
    
    def get_current_pds_size(self) -> float:
        """Get current size of all physical design structures."""
        query = """SELECT (SUM(s.[used_page_count]) * 8)/1024.0 AS size_mb 
                   FROM sys.dm_db_partition_stats AS s"""
        cursor = self._connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0] if result[0] else 0.0
    
    def get_database_size(self) -> float:
        """Get total database size in MB."""
        try:
            query = "EXEC sp_spaceused @oneresultset = 1;"
            cursor = self._connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            # Result format varies by SQL Server version
            size_str = str(result[4]) if len(result) > 4 else str(result[0])
            size_mb = float(size_str.split()[0])
            return size_mb / 1024  # Convert to GB then back? No, keep as MB
        except Exception as e:
            logging.error(f"Exception when getting database size: {e}")
            return 10240  # Default 10GB
    
    def get_selectivity(self, query: str, predicates: Dict[str, List[str]]) -> Dict[str, float]:
        """Calculate selectivity for query predicates."""
        plan_xml = self._get_query_plan_xml(query)
        
        if not plan_xml:
            return {table: 1.0 for table in predicates}
        
        query_plan = QueryPlan(plan_xml)
        
        # Get rows read per table from clustered index scans
        read_rows = {}
        for table in predicates.keys():
            read_rows[table] = float('inf')
        
        for scan in query_plan.clustered_index_usage:
            table_name = scan[0]
            if table_name in read_rows:
                rows_read = float(scan[5]) if len(scan) > 5 else float('inf')
                read_rows[table_name] = min(rows_read, read_rows[table_name])
        
        # Calculate selectivity
        selectivity = {}
        for table in predicates.keys():
            table_row_count = self.get_table_row_count(table)
            selectivity[table] = min(read_rows[table], table_row_count) / table_row_count
        
        return selectivity
    
    def _get_query_plan_xml(self, query: str) -> str:
        """Helper to get query plan XML."""
        cursor = self._connection.cursor()
        cursor.execute("SET SHOWPLAN_XML ON;")
        cursor.execute(query)
        plan_xml = cursor.fetchone()[0]
        cursor.execute("SET SHOWPLAN_XML OFF;")
        return plan_xml
    
    def remove_all_non_clustered_indexes(self) -> None:
        """Remove all non-clustered indexes."""
        query = """SELECT i.name AS index_name, t.name AS table_name
                   FROM sys.indexes i, sys.tables t
                   WHERE i.object_id = t.object_id AND i.type_desc = 'NONCLUSTERED'"""
        
        cursor = self._connection.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        
        for result in results:
            self.drop_index(result[1], result[0])
    
    def restart_server(self) -> None:
        """Restart SQL Server (requires appropriate permissions)."""
        import subprocess
        import time
        import os
        
        try:
            with open(os.devnull, 'w') as devnull:
                subprocess.run("net stop mssqlserver", shell=True, stdout=devnull)
                time.sleep(60)
                subprocess.run("net start mssqlserver", shell=True, stdout=devnull)
            logging.info("SQL Server restarted")
        except Exception as e:
            logging.error(f"Failed to restart SQL Server: {e}")
