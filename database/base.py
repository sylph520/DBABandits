"""
Database abstraction layer for DBA Bandits.

This module defines the interface that all database adapters must implement,
allowing the bandit algorithms to work with different database backends
(MSSQL, PostgreSQL, MySQL, etc.) without code changes.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass


@dataclass
class IndexUsage:
    """Represents index usage statistics from query execution."""
    index_name: str
    table_name: str
    scan_count: int
    elapsed_time: float
    cpu_time: float
    sub_tree_cost: float


@dataclass
class QueryPlanInfo:
    """Parsed query plan information."""
    elapsed_time: float
    cpu_time: float
    est_statement_sub_tree_cost: float
    non_clustered_index_usage: List[Tuple]  # [(index_name, elapsed, cpu, subtree_cost)]
    clustered_index_usage: List[Tuple]  # [(table_name, elapsed, cpu, subtree_cost, rows)]


@dataclass
class TableInfo:
    """Database table metadata."""
    name: str
    row_count: int
    primary_key_columns: List[str]
    columns: Dict[str, 'ColumnInfo']
    
    @property
    def table_row_count(self) -> int:
        """Backward compatibility property."""
        return self.row_count


@dataclass
class ColumnInfo:
    """Database column metadata."""
    name: str
    table_name: str
    data_type: str
    max_size: int
    avg_size: Optional[int] = None


class DatabaseInterface(ABC):
    """
    Abstract interface for database operations.
    
    All database adapters (MSSQL, PostgreSQL, etc.) must implement this interface.
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        """
        Initialize the database adapter.
        
        Args:
            connection_params: Dictionary with connection parameters
                (server, database, username, password, etc.)
        """
        self.connection_params = connection_params
        self._connection = None
        self._table_cache: Dict[str, TableInfo] = {}
    
    @abstractmethod
    def connect(self) -> Any:
        """
        Establish database connection.
        
        Returns:
            Database connection object
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass
    
    @abstractmethod
    def create_index(self, 
                     table_name: str, 
                     column_names: Tuple[str, ...], 
                     index_name: str,
                     include_columns: Tuple[str, ...] = ()) -> float:
        """
        Create a non-clustered index on the specified table.
        
        Args:
            table_name: Name of the table
            column_names: Columns to index (in order)
            index_name: Name for the new index
            include_columns: Columns to include (covering index)
            
        Returns:
            Creation cost (elapsed time or other metric)
        """
        pass
    
    @abstractmethod
    def drop_index(self, table_name: str, index_name: str) -> None:
        """
        Drop an existing index.
        
        Args:
            table_name: Name of the table
            index_name: Name of the index to drop
        """
        pass
    
    @abstractmethod
    def execute_query(self, query: str, clear_cache: bool = True) -> Tuple[float, List[IndexUsage], List[IndexUsage]]:
        """
        Execute a query and return execution statistics.
        
        Args:
            query: SQL query string
            clear_cache: Whether to clear buffer cache before execution
            
        Returns:
            Tuple of (execution_time, non_clustered_index_usage, clustered_index_usage)
        """
        pass
    
    @abstractmethod
    def get_query_plan(self, query: str) -> QueryPlanInfo:
        """
        Get the execution plan for a query without executing it.
        
        Args:
            query: SQL query string
            
        Returns:
            QueryPlanInfo with plan details and estimated costs
        """
        pass
    
    @abstractmethod
    def get_all_columns(self) -> Tuple[Dict[str, List[str]], int]:
        """
        Get all columns in the database.
        
        Returns:
            Tuple of (columns_dict, total_count) where columns_dict is
            {table_name: [column_names]}
        """
        pass
    
    @abstractmethod
    def get_tables(self) -> Dict[str, TableInfo]:
        """
        Get metadata for all tables.
        
        Returns:
            Dictionary of {table_name: TableInfo}
        """
        pass
    
    @abstractmethod
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get the row count for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Number of rows
        """
        pass
    
    @abstractmethod
    def get_primary_key(self, table_name: str) -> List[str]:
        """
        Get primary key columns for a table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            List of primary key column names
        """
        pass
    
    @abstractmethod
    def estimate_index_size(self, 
                           table_name: str, 
                           column_names: Tuple[str, ...],
                           include_columns: Tuple[str, ...] = ()) -> float:
        """
        Estimate the size of an index in MB.
        
        Args:
            table_name: Name of the table
            column_names: Columns in the index
            include_columns: Included columns
            
        Returns:
            Estimated size in MB
        """
        pass
    
    @abstractmethod
    def get_current_pds_size(self) -> float:
        """
        Get current size of all physical design structures (indexes).
        
        Returns:
            Size in MB
        """
        pass
    
    @abstractmethod
    def get_database_size(self) -> float:
        """
        Get total database size.
        
        Returns:
            Size in MB
        """
        pass
    
    @abstractmethod
    def get_selectivity(self, query: str, predicates: Dict[str, List[str]]) -> Dict[str, float]:
        """
        Calculate selectivity for query predicates.
        
        Args:
            query: SQL query string
            predicates: Dict of {table_name: [column_names]} for predicates
            
        Returns:
            Dict of {table_name: selectivity_ratio}
        """
        pass
    
    @abstractmethod
    def remove_all_non_clustered_indexes(self) -> None:
        """Remove all non-clustered indexes from the database."""
        pass
    
    @abstractmethod
    def restart_server(self) -> None:
        """Restart the database server (if supported)."""
        pass
    
    def get_connection(self) -> Any:
        """Get or create database connection."""
        if self._connection is None:
            self._connection = self.connect()
        return self._connection
    
    def cursor(self) -> Any:
        """Get a cursor from the database connection."""
        if self._connection is None:
            self.connect()
        return self._connection.cursor()
    
    def commit(self) -> None:
        """Commit the current transaction."""
        if self._connection is not None:
            self._connection.commit()
