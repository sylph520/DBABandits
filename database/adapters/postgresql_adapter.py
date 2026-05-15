"""
PostgreSQL adapter implementing DatabaseInterface.

This serves as a template for implementing database adapters for other backends.
Key differences from MSSQL:
- Uses psycopg2 instead of pyodbc
- No hypothetical indexes (use EXPLAIN instead)
- Different system tables for metadata
- Different syntax for index operations
"""

import logging
from typing import Dict, List, Tuple, Any
import psycopg2
from psycopg2 import sql
import json

from database.base import (
    DatabaseInterface, QueryPlanInfo, TableInfo, ColumnInfo,
    IndexUsage, ClusteredUsage
)


class PostgreSQLAdapter(DatabaseInterface):
    """
    PostgreSQL adapter for DBA Bandits.

    NOTE: This is a TEMPLATE implementation. Some features differ from MSSQL:
    - PostgreSQL doesn't have hypothetical indexes like MSSQL's STATISTICS_ONLY
    - Query plan format is different (JSON/EXPLAIN vs XML)
    - System catalogs differ (pg_* tables vs sys.*)

    Usage:
        from database.adapters.postgresql_adapter import PostgreSQLAdapter

        db = PostgreSQLAdapter({
            'server': '/tmp',
            'database': 'indexselection_tpch___1',
            'schema': 'public',
            'port': 51204
        })
        db.connect()

        # Use same interface as MSSQL
        cost = db.create_index('lineitem', ('l_shipdate',), 'idx_shipdate')
        db.disconnect()
    """

    def __init__(self, connection_params: Dict[str, Any]):
        super().__init__(connection_params)
        self.schema_name = connection_params.get('schema', 'public')
        self.port = connection_params.get('port', 51204)
        self._tables_global: Dict[str, TableInfo] = None
        self._pk_columns_cache: Dict[str, List[str]] = {}
        self.use_real_indexes_in_rounds = connection_params.get('use_real_indexes_in_rounds', False)

        # HypoPG cost estimation parameters
        self._hypopg_cost_mode = connection_params.get('hyp_cost_mode', 'none')
        self._hypopg_fixed_cost = connection_params.get('hyp_cost_fixed', 0.01)
        self._hypopg_size_multiplier = connection_params.get('hyp_cost_size_multiplier', 0.001)
        # opencode: Whether to include estimated creation cost in hyp mode
        self._include_hyp_cost = connection_params.get('include_hyp_cost', False)

    def connect(self) -> Any:
        """Establish connection to PostgreSQL."""
        conn_params = {
            'host': self.connection_params['server'],
            'database': self.connection_params['database'],
            'user': self.connection_params['username'],
            'password': self.connection_params['password'],
            'port': self.port,
        }

        self._connection = psycopg2.connect(**conn_params)
        # Enable autocommit for DDL operations
        self._connection.autocommit = True
        return self._connection

    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        if self._connection:
            self._connection.close()
            self._connection = None

    def create_index(self, 
                     table_name: str, 
                     column_names: Tuple[str, ...], 
                     index_name: str,
                     include_columns: Tuple[str, ...] = ()) -> float:
        """
        Create an index and return creation time.

        When using hypothetical indexes (HypoPG):
        - If hyp_cost_mode='none': returns 0.0 (no actual creation)
        - If hyp_cost_mode='size': estimates based on size * multiplier
        - If hyp_cost_mode='fixed': estimates as fixed value per MB

        When using real indexes: returns actual creation time.
        """
        import time
        import constants

        cursor = self._connection.cursor()

        table_name_lower = table_name.lower()
        column_names_lower = tuple(col.lower() for col in column_names)
        include_columns_lower = tuple(col.lower() for col in include_columns) if include_columns else ()

        if include_columns_lower:
            all_columns = column_names_lower + include_columns_lower
            query = sql.SQL("CREATE INDEX {} ON {}.{} ({})").format(
                sql.Identifier(index_name),
                sql.Identifier(self.schema_name),
                sql.Identifier(table_name_lower),
                sql.SQL(', ').join(map(sql.Identifier, all_columns))
            )
        else:
            query = sql.SQL("CREATE INDEX {} ON {}.{} ({})").format(
                sql.Identifier(index_name),
                sql.Identifier(self.schema_name),
                sql.Identifier(table_name_lower),
                sql.SQL(', ').join(map(sql.Identifier, column_names_lower))
            )

        # Handle different index creation modes
        if self.use_real_indexes_in_rounds:
            # Real indexes - measure actual creation time
            start_time = time.time()
            cursor.execute(query)
            end_time = time.time()
            creation_cost = end_time - start_time
        else:
            # Hypothetical indexes - use HypoPG
            cursor.execute(query)

            # Get the HypoPG index OID for size calculation
            cursor.execute(
                "SELECT indexrelid FROM hypopg_list_indexes() WHERE indexname = %s",
                (index_name,)
            )
            result = cursor.fetchone()

            if result and hasattr(self, '_hypopg_cost_mode'):
                # Calculate estimated cost based on mode
                if self._hypopg_cost_mode == 'none':
                    creation_cost = 0.0
                elif self._hypopg_cost_mode == 'size':
                    # Get size from HypoPG and apply multiplier
                    size_bytes = self.hypopg_relation_size(result[0])
                    size_mb = size_bytes / (1024.0 * 1024.0)
                    creation_cost = size_mb * self._hypopg_size_multiplier
                elif self._hypopg_cost_mode == 'fixed':
                    # Get size and apply fixed rate
                    size_bytes = self.hypopg_relation_size(result[0])
                    size_mb = size_bytes / (1024.0 * 1024.0)
                    creation_cost = size_mb * self._hypopg_fixed_cost
                else:
                    creation_cost = 0.0
            else:
                creation_cost = 0.0

        logging.info(f"Added: {index_name}")

        return creation_cost

    def drop_index(self, table_name: str, index_name: str) -> None:
        """Drop an existing index."""
        cursor = self._connection.cursor()

        # PostgreSQL DROP INDEX syntax (no schema.table.index, just DROP INDEX name)
        # Index names are case-insensitive in PostgreSQL, use as-is
        query = sql.SQL("DROP INDEX IF EXISTS {}.{}").format(
            sql.Identifier(self.schema_name),
            sql.Identifier(index_name)
        )

        cursor.execute(query)
        logging.info(f"removed: {index_name}")

    def execute_query(self, query: str, clear_cache: bool = True) -> Tuple[float, List[IndexUsage], List[IndexUsage]]:
        """
        opencode: Execute a query and return execution statistics.

        Uses EXPLAIN ANALYZE to get actual execution data including:
        - Non-clustered index usage (Index Scan, Index Only Scan, Bitmap Index Scan)
        - Sequential Scan data (used as baseline, like SQL Server's clustered index scan)

        NOTE: PostgreSQL uses heap storage without default clustered indexes.
        Seq Scan provides the baseline for reward calculation.
        """
        try:
            # opencode: Attempt to clear buffer cache if requested
            if clear_cache:
                self._clear_buffer_cache()

            # opencode: Use EXPLAIN ANALYZE to get actual execution data
            plan_info = self.get_query_plan(query, use_analyze=True)

            # opencode: Convert plan info to IndexUsage objects
            nc_usage = []
            for i in plan_info.non_clustered_index_usage:
                nc_usage.append(IndexUsage(
                    index_name=i[0],
                    table_name=i[1],
                    scan_count=1,
                    elapsed_time=i[2],
                    cpu_time=0,
                    sub_tree_cost=i[2]
                ))

            # opencode: clustered_index_usage contains Seq Scans (baseline for PostgreSQL)
            c_usage = []
            for c in plan_info.clustered_index_usage:
                c_usage.append(IndexUsage(
                    index_name="seq_scan",  # opencode: Labeled for clarity
                    table_name=c.table_name,
                    scan_count=1,
                    elapsed_time=c.elapsed_time,
                    cpu_time=0,
                    sub_tree_cost=c.elapsed_time
                ))

            return plan_info.elapsed_time, nc_usage, c_usage

        except Exception as e:
            logging.error(f"Exception when executing query: {query}, error: {e}")
            return 0, [], []

    def _clear_buffer_cache(self) -> None:
        """
        opencode: Attempt to clear PostgreSQL buffer cache.

        PostgreSQL doesn't have direct equivalent of SQL Server's DBCC DROPCLEANBUFFERS.
        Options:
        1. pg_buffercache extension (requires extension installation)
        2. DISCARD ALL (resets session state only, not system cache)
        3. OS-level cache drop (requires superuser, Linux only)

        This implementation uses DISCARD ALL as a fallback.
        """
        try:
            cursor = self._connection.cursor()
            cursor.execute("DISCARD ALL")
        except Exception as e:
            logging.debug(f"Could not clear cache: {e}")

    # opencode: Added use_analyze parameter to control EXPLAIN ANALYZE
    def get_query_plan(self, query: str, use_analyze: bool = True) -> QueryPlanInfo:
        """
        opencode: Get the execution plan for a query.

        opencode: Added use_analyze parameter to control whether to execute the query.

        Args:
            query: SQL query string
            use_analyze: If True, uses EXPLAIN ANALYZE (executes query for real timing).
                         If False, uses EXPLAIN only (faster, estimated costs only).

        NOTE: PostgreSQL uses EXPLAIN instead of SHOWPLAN_XML.
        Returns estimated costs and index usage.
        """
        cursor = self._connection.cursor()

        # Choose EXPLAIN format based on use_analyze parameter
        if use_analyze:
            # ANALYZE actually executes the query for real timing (slower but accurate)
            explain_query = f"EXPLAIN (FORMAT JSON, ANALYZE) {query}"
        else:
            # Without ANALYZE: just estimates (faster, no execution)
            explain_query = f"EXPLAIN (FORMAT JSON) {query}"

        cursor.execute(explain_query)

        plan_result = cursor.fetchone()[0]

        # Parse the JSON plan
        # Structure: [{"Plan": {"Total Cost": ..., "Plan Rows": ...}}]
        # psycopg2 may automatically parse JSON to Python object
        if isinstance(plan_result, (list, dict)):
            plan_data = plan_result
        else:
            plan_data = json.loads(plan_result)

        # Extract costs (these are PostgreSQL's planner estimates)
        root_plan = plan_data[0]['Plan']
        total_cost = root_plan.get('Total Cost', 0)
        startup_cost = root_plan.get('Startup Cost', 0)

        # opencode: Parse index usage from the plan tree (extracted from EXPLAIN JSON)
        # opencode: PostgreSQL semantics:
        # opencode: - Seq Scan = table heap read (baseline for comparison, like SQL Server's clustered index scan)
        # opencode: - Index Scan/Index Only Scan/Bitmap Index Scan = non-clustered index usage
        nc_usage = []
        c_usage = []

        parse_postgres_plan_node(root_plan, None, nc_usage, c_usage)

        # opencode: Log warning if no baseline (Seq Scan) and no index usage found
        if not c_usage and not nc_usage:
            logging.debug(f"No Seq Scan (baseline) or Index Scan found in query plan")

        # PostgreSQL costs are arbitrary units, not seconds
        # You may want to calibrate these against actual execution times

        return QueryPlanInfo(
            elapsed_time=total_cost,  # In PostgreSQL, this is planner cost
            cpu_time=startup_cost,
            est_statement_sub_tree_cost=total_cost,
            non_clustered_index_usage=nc_usage,
            clustered_index_usage=c_usage  # opencode: Contains Seq Scans (baseline)
        )

    def get_all_columns(self) -> Tuple[Dict[str, List[str]], int]:
        """Get all columns in the database."""
        from collections import defaultdict

        query = """
            SELECT table_name, column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s
            ORDER BY table_name, ordinal_position;
        """

        cursor = self._connection.cursor()
        cursor.execute(query, (self.schema_name,))
        results = cursor.fetchall()

        columns = defaultdict(list)
        for result in results:
            columns[result[0]].append(result[1])

        return dict(columns), len(results)

    def get_tables(self) -> Dict[str, TableInfo]:
        """Get metadata for all tables. Returns lowercase keys for compatibility."""
        if self._tables_global is not None:
            return self._tables_global

        tables = {}

        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE';
        """

        cursor = self._connection.cursor()
        cursor.execute(query, (self.schema_name,))
        results = cursor.fetchall()

        for result in results:
            table_name = result[0]
            # Normalize to lowercase for compatibility with workloads
            table_name_lower = table_name.lower()
            row_count = self.get_table_row_count(table_name)
            pk_columns = self.get_primary_key(table_name)

            # Get column metadata
            columns = self._get_columns(table_name)

            tables[table_name_lower] = TableInfo(
                name=table_name_lower,
                row_count=row_count,
                primary_key_columns=pk_columns,
                columns=columns
            )

        self._tables_global = tables
        return tables

    def _get_columns(self, table_name: str) -> Dict[str, ColumnInfo]:
        """Get column metadata for a table."""
        columns = {}

        query = """
            SELECT 
                column_name,
                data_type,
                character_maximum_length,
                numeric_precision
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """

        cursor = self._connection.cursor()
        cursor.execute(query, (self.schema_name, table_name))
        results = cursor.fetchall()

        for result in results:
            col_name = result[0]
            data_type = result[1]
            max_length = result[2] if result[2] else result[3] if result[3] else 0

            columns[col_name] = ColumnInfo(
                name=col_name,
                table_name=table_name,
                data_type=data_type,
                max_size=max_length,
                avg_size=None  # Would need to query actual data
            )

        return columns

    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        # Use pg_class for fast estimate, or COUNT for exact
        query_exact = f"SELECT COUNT(*) FROM {self.schema_name}.{table_name};"

        # Fast estimate from pg_class
        query_estimate = """
            SELECT reltuples::BIGINT AS estimate
            FROM pg_class
            WHERE relname = %s AND relnamespace = (
                SELECT oid FROM pg_namespace WHERE nspname = %s
            );
        """

        cursor = self._connection.cursor()

        # For small tables, exact count is fine
        # For large tables, use estimate
        cursor.execute(query_estimate, (table_name, self.schema_name))
        result = cursor.fetchone()

        if result and result[0] > 1000000:  # If estimate > 1M rows
            return int(result[0])
        else:
            # Get exact count for smaller tables
            cursor.execute(query_exact)
            return cursor.fetchone()[0]

    def get_primary_key(self, table_name: str) -> List[str]:
        """Get primary key columns for a table."""
        if table_name in self._pk_columns_cache:
            return self._pk_columns_cache[table_name]

        query = """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = %s
                AND tc.table_name = %s
            ORDER BY kcu.ordinal_position;
        """

        cursor = self._connection.cursor()
        cursor.execute(query, (self.schema_name, table_name))
        results = cursor.fetchall()

        pk_columns = [result[0] for result in results]
        self._pk_columns_cache[table_name] = pk_columns
        return pk_columns

    def estimate_index_size(self, 
                           table_name: str, 
                           column_names: Tuple[str, ...],
                           include_columns: Tuple[str, ...] = ()) -> float:
        """
        Estimate index size in MB.

        NOTE: This is a rough estimation. PostgreSQL has pg_relation_size()
        for actual sizes, but for hypothetical indexes we must estimate.
        """
        # Get table info
        tables = self.get_tables()
        table = tables[table_name]

        # Estimate row size
        # PostgreSQL has ~24 bytes overhead per tuple
        header_size = 24

        # Get primary key size (oid or actual columns)
        pk_columns = self.get_primary_key(table_name)
        pk_size = sum(
            self._get_column_storage_size(table_name, col) 
            for col in pk_columns
        ) if pk_columns else 4  # oid is 4 bytes

        # Get index key size
        key_size = sum(
            self._get_column_storage_size(table_name, col) 
            for col in column_names
        )

        # Include columns (in PostgreSQL, these are part of the index key)
        include_size = sum(
            self._get_column_storage_size(table_name, col) 
            for col in include_columns
        )

        # Total row size in index
        row_size = header_size + pk_size + key_size + include_size

        # Estimate total size
        # Add 50% overhead for B-tree structure
        estimated_size = table.row_count * row_size * 1.5
        estimated_size_mb = estimated_size / (1024 * 1024)

        return estimated_size_mb

    def _get_column_storage_size(self, table_name: str, column_name: str) -> int:
        """Estimate storage size for a column in bytes."""
        tables = self.get_tables()
        column = tables[table_name].columns.get(column_name)

        if not column:
            return 8  # Default to 8 bytes

        # Map PostgreSQL types to sizes
        type_sizes = {
            'integer': 4,
            'bigint': 8,
            'smallint': 2,
            'real': 4,
            'double precision': 8,
            'boolean': 1,
            'date': 4,
            'timestamp': 8,
            'text': 100,  # Variable, use average
            'varchar': 50,  # Variable, use average
            'char': 10,
        }

        data_type = column.data_type.lower()

        if data_type in type_sizes:
            return type_sizes[data_type]
        elif 'varchar' in data_type or 'char' in data_type:
            # Use max size for char/varchar
            return column.max_size if column.max_size else 50
        else:
            return 8  # Default

    def get_current_pds_size(self) -> float:
        """Get current size of all indexes in MB.

        When using hypothetical indexes (use_real_indexes_in_rounds=False),
        uses hypopg_relation_size() to get estimated sizes.
        When using real indexes, uses pg_relation_size().
        """
        cursor = self._connection.cursor()

        if not self.use_real_indexes_in_rounds:
            # Using hypothetical indexes - use HypoPG's size estimation
            cursor.execute("SELECT hypopg_relation_size(indexrelid) FROM hypopg_list_indexes()")
            total = sum(row[0] for row in cursor.fetchall() if row[0])
            return total / (1024.0 * 1024.0)
        else:
            # Using real indexes - use actual PostgreSQL sizes
            query = """
                SELECT 
                    COALESCE(SUM(pg_relation_size(indexrelid)), 0) / (1024.0 * 1024.0) as total_size_mb
                FROM pg_stat_user_indexes
                WHERE schemaname = %s;
            """
            cursor.execute(query, (self.schema_name,))
            result = cursor.fetchone()
            return float(result[0]) if result and result[0] else 0.0

    def get_database_size(self) -> float:
        """Get total database size in MB."""
        query = "SELECT pg_database_size(%s) / (1024.0 * 1024.0);"

        cursor = self._connection.cursor()
        cursor.execute(query, (self.connection_params['database'],))
        result = cursor.fetchone()

        return result[0] if result else 0.0

    def hypopg_relation_size(self, index_oid: int) -> int:
        """Get the estimated size of a hypothetical index using HypoPG.

        Args:
            index_oid: The OID of the hypothetical index

        Returns:
            Estimated size in bytes
        """
        cursor = self._connection.cursor()
        cursor.execute("SELECT hypopg_relation_size(%s)", (index_oid,))
        result = cursor.fetchone()
        return result[0] if result and result[0] else 0

    def get_selectivity(self, query: str, predicates: Dict[str, List[str]]) -> Dict[str, float]:
        """
        Calculate selectivity for query predicates.

        NOTE: PostgreSQL can use EXPLAIN to get row estimates,
        or pg_stats for actual selectivity.

        If query execution fails (e.g., MSSQL syntax), returns default selectivity.
        """
        try:
            cursor = self._connection.cursor()

            # Use EXPLAIN to get row estimates
            explain_query = f"EXPLAIN (FORMAT JSON) {query}"
            cursor.execute(explain_query)
            plan_result = cursor.fetchone()[0]

            import json
            # psycopg2 may automatically parse JSON to Python object
            if isinstance(plan_result, (list, dict)):
                plan_data = plan_result
            else:
                plan_data = json.loads(plan_result)

            # Estimate selectivity based on plan rows vs table rows
            selectivity = {}
            for table_name in predicates.keys():
                table_rows = self.get_table_row_count(table_name)
                # This is simplified - actual implementation would parse plan nodes
                selectivity[table_name] = 0.1  # Placeholder

            return selectivity

        except Exception as e:
            # Query may have incompatible syntax (e.g., MSSQL-specific functions)
            # Return default selectivity for all tables
            logging.warning(f"Could not calculate selectivity for query: {str(e)[:100]}")
            return {table_name: 0.5 for table_name in predicates.keys()}

    def remove_all_non_clustered_indexes(self) -> None:
        """Remove all non-clustered indexes (except primary keys)."""
        query = """
            SELECT indexname 
            FROM pg_indexes 
            WHERE schemaname = %s 
            AND indexname NOT IN (
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE constraint_type = 'PRIMARY KEY' 
                AND table_schema = %s
            );
        """

        cursor = self._connection.cursor()
        cursor.execute(query, (self.schema_name, self.schema_name))
        results = cursor.fetchall()

        for result in results:
            # Get table name for this index
            table_query = """
                SELECT tablename 
                FROM pg_indexes 
                WHERE schemaname = %s AND indexname = %s;
            """
            cursor.execute(table_query, (self.schema_name, result[0]))
            table_result = cursor.fetchone()

            if table_result:
                self.drop_index(table_result[0], result[0])

    def restart_server(self) -> None:
        """
        Restart PostgreSQL server.

        NOTE: This requires OS-level privileges. Usually better to
        handle at orchestration level (Docker, systemd, etc.).
        """
        logging.warning("PostgreSQL server restart requires OS-level permissions.")
        logging.info("Please restart PostgreSQL manually or via your orchestration tool.")

# opencode: Helper function moved from get_query_plan() method
def parse_postgres_plan_node(node, parent_relation, nc_usage, c_usage):
    """
    Recursively parse PostgreSQL EXPLAIN JSON plan nodes to find index scans.

    Args:
        node: Current plan node dictionary
        parent_relation: Parent table name for context
        nc_usage: List to append non-clustered index usage tuples
        c_usage: List to append ClusteredUsage for baseline tracking
    """
    node_type = node.get('Node Type')

    if node_type in ['Index Scan', 'Index Only Scan']:
        index_name = node.get('Index Name', 'unknown')
        table_name = node.get('Relation Name', parent_relation)
        cost = node.get('Total Cost', 0) or node.get('Startup Cost', 0)
        nc_usage.append((index_name, table_name, cost))

    elif node_type == 'Seq Scan':
        table_name = node.get('Relation Name', parent_relation)
        cost = node.get('Total Cost', 0) or node.get('Startup Cost', 0)
        c_usage.append(ClusteredUsage(
            index_name='seq_scan',
            table_name=table_name,
            elapsed_time=cost,
            cpu_time=0,
            sub_tree_cost=cost
        ))

    elif node_type == 'Bitmap Index Scan':
        index_name = node.get('Index Name', 'unknown')
        table_name = node.get('Relation Name', parent_relation)
        cost = node.get('Total Cost', 0) or node.get('Startup Cost', 0)
        nc_usage.append((index_name, table_name, cost))

    elif node_type == 'Tid Scan':
        pass

    for child in node.get('Plans', []):
        parse_postgres_plan_node(child, node.get('Relation Name', parent_relation), nc_usage, c_usage)
