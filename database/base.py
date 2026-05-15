# opencode: NEW FILE - Database abstraction layer for DBA Bandits
"""
Database abstraction layer for DBA Bandits.

opencode: NEW FILE - This module defines the interface that all database adapters must implement,
allowing the bandit algorithms to work with different database backends
(MSSQL, PostgreSQL, MySQL, etc.) without code changes.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass


# opencode: NEW DATACLASS - Index usage statistics
@dataclass
class IndexUsage:
    """Represents index usage statistics from query execution."""
    index_name: str
    table_name: str
    scan_count: int
    elapsed_time: float
    cpu_time: float
    sub_tree_cost: float


# opencode: NEW DATACLASS - Clustered/Seq scan usage (for baseline tracking)
# opencode: PostgreSQL: Seq Scan represents baseline (like SQL Server's clustered index scan)
@dataclass
class ClusteredUsage:
    """Represents baseline scan statistics (Seq Scan in PostgreSQL, Clustered Index Scan in SQL Server)."""
    index_name: str  # opencode: 'seq_scan' for PostgreSQL, 'clustered' for SQL Server
    table_name: str
    elapsed_time: float
    cpu_time: float = 0
    sub_tree_cost: float = 0


# opencode: Updated non_clustered_index_usage to return (index_name, table_name, cost) tuples
@dataclass
class QueryPlanInfo:
    """Parsed query plan information."""
    elapsed_time: float
    cpu_time: float
    est_statement_sub_tree_cost: float
    # opencode: Changed format to [(index_name, table_name, cost), ...]
    non_clustered_index_usage: List[Tuple]  # [(index_name, table_name, cost), ...]
    # opencode: Changed to List[ClusteredUsage] for consistent attribute access
    clustered_index_usage: List[ClusteredUsage]

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


# opencode: NEW DATACLASS - Column metadata
@dataclass
class ColumnInfo:
    """Database column metadata."""
    name: str
    table_name: str
    data_type: str
    max_size: int
    avg_size: Optional[int] = None


# opencode: NEW CLASS - Abstract database interface
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

    # opencode: NEW METHOD - Connect to database
    @abstractmethod
    def connect(self) -> Any:
        pass

    # opencode: NEW METHOD - Disconnect from database
    @abstractmethod
    def disconnect(self) -> None:
        pass

    # opencode: NEW METHOD - Create index
    @abstractmethod
    def create_index(self, 
                     table_name: str, 
                     column_names: Tuple[str, ...], 
                     index_name: str,
                     include_columns: Tuple[str, ...] = ()) -> float:
        pass

    # opencode: NEW METHOD - Drop index
    @abstractmethod
    def drop_index(self, table_name: str, index_name: str) -> None:
        pass

    # opencode: NEW METHOD - Execute query with statistics
    @abstractmethod
    def execute_query(self, query: str, clear_cache: bool = True) -> Tuple[float, List[IndexUsage], List[IndexUsage]]:
        pass

    # opencode: Added use_analyze parameter to base interface
    @abstractmethod
    def get_query_plan(self, query: str, use_analyze: bool = True) -> QueryPlanInfo:
        pass

    # opencode: NEW METHOD - Get all columns
    @abstractmethod
    def get_all_columns(self) -> Tuple[Dict[str, List[str]], int]:
        pass

    # opencode: NEW METHOD - Get all tables metadata
    @abstractmethod
    def get_tables(self) -> Dict[str, TableInfo]:
        pass

    # opencode: NEW METHOD - Get table row count
    @abstractmethod
    def get_table_row_count(self, table_name: str) -> int:
        pass

    # opencode: NEW METHOD - Get primary key columns
    @abstractmethod
    def get_primary_key(self, table_name: str) -> List[str]:
        pass

    # opencode: NEW METHOD - Estimate index size
    @abstractmethod
    def estimate_index_size(self, 
                           table_name: str, 
                           column_names: Tuple[str, ...],
                           include_columns: Tuple[str, ...] = ()) -> float:
        pass

    # opencode: NEW METHOD - Get current PDS size
    @abstractmethod
    def get_current_pds_size(self) -> float:
        pass

    # opencode: NEW METHOD - Get database size
    @abstractmethod
    def get_database_size(self) -> float:
        pass

    # opencode: NEW METHOD - Get selectivity
    @abstractmethod
    def get_selectivity(self, query: str, predicates: Dict[str, List[str]]) -> Dict[str, float]:
        pass

    # opencode: NEW METHOD - Remove all non-clustered indexes
    @abstractmethod
    def remove_all_non_clustered_indexes(self) -> None:
        pass

    # opencode: NEW METHOD - Restart server
    @abstractmethod
    def restart_server(self) -> None:
        pass

    # opencode: NEW METHOD - Get connection (lazy connection)
    def get_connection(self) -> Any:
        """Get or create database connection."""
        if self._connection is None:
            self._connection = self.connect()
        return self._connection

    # opencode: NEW METHOD - Get cursor
    def cursor(self) -> Any:
        """Get a cursor from the database connection."""
        if self._connection is None:
            self.connect()
        return self._connection.cursor()

    # opencode: NEW METHOD - Commit transaction
    def commit(self) -> None:
        """Commit the current transaction."""
        if self._connection is not None:
            self._connection.commit()
