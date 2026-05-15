"""
HypoPG Adapter for PostgreSQL - Hypothetical Index Support

This adapter extends the PostgreSQL adapter to use HypoPG extension
for hypothetical indexes, enabling fast what-if analysis without
actually creating indexes.

HypoPG allows:
- Creating hypothetical indexes (not persisted)
- Getting query plans with hypothetical indexes
- Estimating index benefits without overhead

Usage:
    from database.adapters.hypopg_adapter import HypoPGAdapter
    
    db = HypoPGAdapter({
        'server': '/tmp',
        'database': 'tpch',
        'username': 'sclai',
        'password': '',
        'port': 51204,
        'schema': 'public'
    })
    db.connect()
    
    # Enable HypoPG extension
    db.enable_hypopg()
    
    # Create hypothetical index
    index_id = db.create_hypothetical_index('lineitem', ('l_shipdate',), 'idx_test')
    
    # Get query plan with hypothetical index
    plan = db.get_query_plan_with_hypothetical_index('SELECT * FROM lineitem', index_id)
    
    # Remove hypothetical index
    db.drop_hypothetical_index(index_id)
"""

# opencode
import logging
from typing import Dict, List, Tuple, Any, Optional

from database.adapters.postgresql_adapter import PostgreSQLAdapter
from database.base import QueryPlanInfo


# opencode
class HypoPGAdapter(PostgreSQLAdapter):
    """
    PostgreSQL adapter with HypoPG extension for hypothetical indexes.
    
    This provides the same interface as PostgreSQLAdapter but uses
    HypoPG for index operations, allowing what-if analysis without
    the overhead of actually creating indexes.
    """
    
    def __init__(self, connection_params: Dict[str, Any]):
        super().__init__(connection_params)
        self.hypopg_enabled = False
        self.hypothetical_indexes: Dict[str, int] = {}  # name -> index_id
        
    def connect(self) -> Any:
        """Connect and check if HypoPG is available."""
        super().connect()
        self._check_hypopg_available()
        return self._connection
    
    def _check_hypopg_available(self):
        """Check if HypoPG extension is installed."""
        try:
            cursor = self._connection.cursor()
            cursor.execute("SELECT * FROM pg_available_extensions WHERE name = 'hypopg'")
            result = cursor.fetchone()
            if result:
                logging.info("HypoPG extension is available")
                self.hypopg_available = True
            else:
                logging.warning("HypoPG extension not available. Install with: CREATE EXTENSION hypopg;")
                self.hypopg_available = False
        except Exception as e:
            logging.warning(f"Could not check HypoPG availability: {e}")
            self.hypopg_available = False
    
    def enable_hypopg(self):
        """Enable HypoPG extension in the current session."""
        if not self.hypopg_available:
            raise RuntimeError("HypoPG extension not available")
        
        cursor = self._connection.cursor()
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS hypopg")
            self._connection.commit()
            self.hypopg_enabled = True
            logging.info("HypoPG extension enabled")
        except Exception as e:
            logging.error(f"Failed to enable HypoPG: {e}")
            raise
    
    def create_index(self, 
                     table_name: str, 
                     column_names: Tuple[str, ...], 
                     index_name: str,
                     include_columns: Tuple[str, ...] = ()) -> float:
        """
        Create a hypothetical index using HypoPG.
        
        Returns:
            Estimated creation cost (0 for hypothetical indexes)
        """
        if not self.hypopg_enabled:
            return super().create_index(table_name, column_names, index_name, include_columns)
        
        try:
            cursor = self._connection.cursor()
            
            table_name_lower = table_name.lower()
            column_names_lower = tuple(col.lower() for col in column_names)
            include_columns_lower = tuple(col.lower() for col in include_columns) if include_columns else ()
            
            # opencode: HypoPG does not support user-assigned index names, uses auto-generated <oid>btree_... format
            if include_columns_lower:
                all_columns = column_names_lower + include_columns_lower
                index_def = f"CREATE INDEX ON {self.schema_name}.{table_name_lower} ({', '.join(column_names_lower)}) INCLUDE ({', '.join(include_columns_lower)})"
            else:
                index_def = f"CREATE INDEX ON {self.schema_name}.{table_name_lower} ({', '.join(column_names_lower)})"
            
            cursor.execute("SELECT (hypopg_create_index(%s)).indexrelid", (index_def,))
            result = cursor.fetchone()
            index_id = result[0]
            
            self.hypothetical_indexes[index_name] = index_id
            logging.debug(f"Created hypothetical index {index_name} (ID: {index_id})")
            
            # opencode: Optionally include estimated creation cost for hyp indexes
            if getattr(self, '_include_hyp_cost', False) and self._hypopg_cost_mode != 'none':
                cursor.execute("SELECT hypopg_relation_size(%s)", (index_id,))
                size_result = cursor.fetchone()
                size_bytes = size_result[0] if size_result and size_result[0] else 0
                size_mb = size_bytes / (1024.0 * 1024.0)
                if self._hypopg_cost_mode == 'size':
                    creation_cost = size_mb * self._hypopg_size_multiplier
                elif self._hypopg_cost_mode == 'fixed':
                    creation_cost = size_mb * self._hypopg_fixed_cost
                else:
                    creation_cost = 0.0
            else:
                creation_cost = 0.0
            return creation_cost
            
        except Exception as e:
            logging.error(f"Failed to create hypothetical index: {e}")
            raise
    
    # opencode: Override get_query_plan to translate HypoPG-generated index names back to arm names.
    # HypoPG auto-generates names in <oid>btree_<tablename>_<col1>_<col2>... format (ignoring user-provided names).
    # This mapping uses hypopg_list_indexes() + self.hypothetical_indexes (arm_name -> oid) to translate.
    def get_query_plan(self, query: str, use_analyze: bool = True) -> QueryPlanInfo:
        plan_info = super().get_query_plan(query, use_analyze)
        if not self.hypothetical_indexes:
            return plan_info
        cursor = self._connection.cursor()
        cursor.execute("SELECT indexrelid, indexname FROM hypopg_list_indexes()")
        oid_to_hyp_name = {r[0]: r[1] for r in cursor.fetchall()}
        oid_to_arm = {v: k for k, v in self.hypothetical_indexes.items()}
        hyp_to_arm = {}
        for oid, hyp_name in oid_to_hyp_name.items():
            arm_name = oid_to_arm.get(oid)
            if arm_name:
                hyp_to_arm[hyp_name] = arm_name
        translated = [(hyp_to_arm.get(name, name), table, cost) for name, table, cost in plan_info.non_clustered_index_usage]
        plan_info.non_clustered_index_usage = translated
        return plan_info
    
    def drop_index(self, table_name: str, index_name: str) -> None:
        """
        Drop a hypothetical index.
        """
        if index_name in self.hypothetical_indexes:
            try:
                index_id = self.hypothetical_indexes[index_name]
                cursor = self._connection.cursor()
                cursor.execute("SELECT hypopg_drop_index(%s)", (index_id,))
                del self.hypothetical_indexes[index_name]
                logging.debug(f"Dropped hypothetical index {index_name}")
            except Exception as e:
                logging.error(f"Failed to drop hypothetical index {index_name}: {e}")
                raise
        else:
            super().drop_index(table_name, index_name)
    
    def get_query_plan_with_hypothetical_indexes(self, query: str) -> Dict[str, Any]:
        """
        Get query plan considering all hypothetical indexes.
        
        Returns:
            Query plan dict with estimated costs
        """
        if not self.hypopg_enabled:
            return super().get_query_plan(query)
        
        cursor = self._connection.cursor()
        
        # Get plan with hypothetical indexes
        cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
        plan_result = cursor.fetchone()[0]
        
        import json
        # psycopg2 may automatically parse JSON to Python object
        if isinstance(plan_result, (list, dict)):
            plan_data = plan_result
        else:
            plan_data = json.loads(plan_result)
        
        return {
            'plan': plan_data,
            'total_cost': plan_data[0]['Plan']['Total Cost'],
            'startup_cost': plan_data[0]['Plan']['Startup Cost'],
        }
    
    def remove_all_non_clustered_indexes(self) -> None:
        """Remove all hypothetical and real non-clustered indexes."""
        # First drop all hypothetical indexes
        if self.hypopg_enabled:
            for index_name, index_id in list(self.hypothetical_indexes.items()):
                try:
                    cursor = self._connection.cursor()
                    cursor.execute("SELECT hypopg_drop_index(%s)", (index_id,))
                    logging.info(f"Dropped hypothetical index {index_name}")
                except Exception as e:
                    logging.error(f"Failed to drop hypothetical index {index_name}: {e}")
            self.hypothetical_indexes.clear()
        
        # Then drop real indexes
        super().remove_all_non_clustered_indexes()
    
    def estimate_index_benefit(self, 
                             query: str, 
                             table_name: str, 
                             column_names: Tuple[str, ...]) -> float:
        """
        Estimate the benefit of an index without creating it.
        
        Returns:
            Estimated time reduction (positive = beneficial)
        """
        if not self.hypopg_enabled:
            logging.warning("HypoPG not enabled, cannot estimate index benefit")
            return 0.0
        
        try:
            cursor = self._connection.cursor()
            
            # Get baseline plan
            cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
            baseline_result = cursor.fetchone()[0]
            import json
            # psycopg2 may automatically parse JSON to Python object
            if isinstance(baseline_result, (list, dict)):
                baseline_data = baseline_result
            else:
                baseline_data = json.loads(baseline_result)
            baseline_cost = baseline_data[0]['Plan']['Total Cost']
            
            # Create hypothetical index
            index_def = f"CREATE INDEX ON {self.schema_name}.{table_name} ({', '.join(column_names)})"
            cursor.execute("SELECT hypopg_create_index(%s)", (index_def,))
            result = cursor.fetchone()
            index_id = result[0]
            
            # Get plan with hypothetical index
            cursor.execute(f"EXPLAIN (FORMAT JSON) {query}")
            with_index_result = cursor.fetchone()[0]
            # psycopg2 may automatically parse JSON to Python object
            if isinstance(with_index_result, (list, dict)):
                with_index_data = with_index_result
            else:
                with_index_data = json.loads(with_index_result)
            with_index_cost = with_index_data[0]['Plan']['Total Cost']
            
            # Drop hypothetical index
            cursor.execute("SELECT hypopg_drop_index(%s)", (index_id,))
            
            # Return benefit (positive = cost reduction)
            benefit = baseline_cost - with_index_cost
            logging.info(f"Estimated index benefit: {benefit:.2f} (baseline: {baseline_cost:.2f}, with index: {with_index_cost:.2f})")
            return benefit
            
        except Exception as e:
            logging.error(f"Failed to estimate index benefit: {e}")
            return 0.0
    
    def get_current_pds_size(self) -> float:
        """
        Get size of real indexes only (hypothetical indexes don't consume space).
        """
        # Hypothetical indexes don't consume space, so just get real indexes
        return super().get_current_pds_size()
