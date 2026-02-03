import sys
import os

# Force unbuffered output
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("DEBUG: Script starting...", flush=True)

import datetime
import logging
import operator
import pprint
from importlib import reload
from typing import Dict, Optional

import numpy
import pandas as pd
from pandas import DataFrame

import bandits.bandit_c3ucb_v2 as bandits
import bandits.bandit_helper_v2 as bandit_helper
import constants as constants
import database.sql_connection as sql_connection
import database.sql_helper_v2 as sql_helper
import shared.configs_v2 as configs
import shared.helper as helper
from bandits.experiment_report import ExpReport
from bandits.oracle_v2 import OracleV7 as Oracle
from bandits.query_v5 import Query
from database import create_db_adapter_from_config, DatabaseInterface


# Simulation built on vQ to collect the super arm performance
# Now supports both MSSQL and PostgreSQL via database abstraction layer


class BaseSimulator:
    def __init__(self, db_adapter: Optional[DatabaseInterface] = None, hypopg_available: bool = False, use_optimizer_costs: bool = True):
        """
        setup queries (self.queries), db connection (self.connection),
        and an empty query_object_store
        
        Args:
            db_adapter: Optional pre-configured database adapter. If not provided,
                       will create one from config (supports MSSQL and PostgreSQL)
            hypopg_available: Whether HypoPG extension is available for hypothetical indexes
            use_optimizer_costs: If True, use EXPLAIN optimizer cost estimates instead of 
                                actual query execution (faster for testing)
        """
        self.hypopg_available = hypopg_available
        self.use_optimizer_costs = use_optimizer_costs
        # configuring the logger
        logging.basicConfig(
            filename=helper.get_experiment_folder_path(configs.experiment_id) + configs.experiment_id + '.log',
            filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
        logging.getLogger().setLevel(logging.INFO)

        # Get the query List
        self.queries = helper.get_queries_v2()
        
        # Support both old-style connection and new abstraction layer
        if db_adapter is not None:
            self.db = db_adapter
            self.uses_adapter = True
            if not self.db._connection:
                self.db.connect()
            # For Query class compatibility, use db as connection
            self.connection = self.db
        else:
            # Legacy MSSQL mode
            self.connection = sql_connection.get_sql_connection()
            self.db = None
            self.uses_adapter = False
            
        self.query_obj_store: Dict[int, Query] = {}
        reload(bandit_helper)


class Simulator(BaseSimulator):
    # Simulator inherit from BaseSimulator (init queries and db connections)
    def run(self):
        pp = pprint.PrettyPrinter()
        reload(configs)

        # Check if using PostgreSQL without HypoPG and warn about hyp_rounds
        if self.uses_adapter and configs.hyp_rounds != 0 and not self.hypopg_available:
            logging.warning("PostgreSQL without HypoPG detected - setting hyp_rounds to 0")
            print("⚠️  WARNING: hyp_rounds set to 0 (HypoPG not available)")
            configs.hyp_rounds = 0

        results = []

        # Helper methods for database operations (adapter or legacy)
        def _get_db_connection():
            """Get database connection - adapter or legacy."""
            if self.uses_adapter:
                return self.db
            return self.connection

        def _get_all_columns():
            """Get all columns - adapter or legacy."""
            if self.uses_adapter:
                return self.db.get_all_columns()
            return sql_helper.get_all_columns(self.connection)

        def _get_current_pds_size():
            """Get current PDS size - adapter or legacy."""
            if self.uses_adapter:
                return self.db.get_current_pds_size()
            return sql_helper.get_current_pds_size(self.connection)

        def _create_query_drop(chosen_arms, added_arms, deleted_arms, queries, t):
            """Create indexes, execute queries, drop indexes - adapter or legacy."""
            if self.uses_adapter:
                # Use adapter methods
                # Drop removed indexes
                for index_name, bandit_arm in deleted_arms.items():
                    self.db.drop_index(bandit_arm.table_name, bandit_arm.index_name)
                
                # Create new indexes
                creation_cost = {}
                for index_name, bandit_arm in added_arms.items():
                    cost = self.db.create_index(
                        bandit_arm.table_name,
                        bandit_arm.index_cols,
                        bandit_arm.index_name,
                        bandit_arm.include_cols
                    )
                    creation_cost[index_name] = cost
                
                # Execute queries and calculate rewards
                execute_cost = 0
                arm_rewards = {}
                
                if self.use_optimizer_costs:
                    # Use EXPLAIN optimizer costs instead of actual execution
                    # This is much faster for testing
                    for query in queries:
                        # Get query plan with estimated costs
                        plan_info = self.db.get_query_plan(query.query_string)
                        # PostgreSQL costs are in arbitrary units, use total_cost
                        time_taken = plan_info.est_statement_sub_tree_cost
                        execute_cost += time_taken
                        
                        # For optimizer-only mode, assume all chosen indexes are used
                        # This is a simplification - in real execution we'd check nc_usage
                        for idx_name in chosen_arms.keys():
                            if idx_name not in arm_rewards:
                                arm_rewards[idx_name] = [0, 0]
                            arm_rewards[idx_name][0] += time_taken / len(chosen_arms)
                else:
                    # Actual query execution (slower but real metrics)
                    for query in queries:
                        time_taken, nc_usage, c_usage = self.db.execute_query(query.query_string, clear_cache=False)
                        execute_cost += time_taken
                        # Simplified reward calculation for adapter
                        for idx_use in nc_usage:
                            if idx_use.index_name in chosen_arms:
                                if idx_use.index_name not in arm_rewards:
                                    arm_rewards[idx_use.index_name] = [0, 0]
                                arm_rewards[idx_use.index_name][0] += time_taken  # Simplified
                
                # Add creation costs
                for key, cost in creation_cost.items():
                    if key in arm_rewards:
                        arm_rewards[key][1] += -1 * cost
                    else:
                        arm_rewards[key] = [0, -1 * cost]
                
                return execute_cost, creation_cost, arm_rewards
            else:
                # Legacy MSSQL mode
                if t < configs.hyp_rounds:
                    return sql_helper.hyp_create_query_drop_v2(
                        self.connection, constants.SCHEMA_NAME,
                        chosen_arms, added_arms, deleted_arms, queries
                    )
                else:
                    return sql_helper.create_query_drop_v3(
                        self.connection, constants.SCHEMA_NAME,
                        chosen_arms, added_arms, deleted_arms, queries
                    )

        def _bulk_drop_index(bandit_arms):
            """Drop multiple indexes - adapter or legacy."""
            if self.uses_adapter:
                for index_name, bandit_arm in bandit_arms.items():
                    self.db.drop_index(bandit_arm.table_name, bandit_arm.index_name)
            else:
                sql_helper.bulk_drop_index(self.connection, constants.SCHEMA_NAME, bandit_arms)

        def _restart_server():
            """Restart server - adapter or legacy."""
            if self.uses_adapter and hasattr(self, 'use_optimizer_costs') and not self.use_optimizer_costs:
                # Only restart if actually executing queries (not using optimizer costs only)
                self.db.restart_server()
            elif self.uses_adapter:
                logging.info("Skipping server restart (optimizer cost mode - no state to clear)")
            else:
                sql_helper.restart_sql_server()

        def _create_query_postgres(query_id, query_string, predicates, payloads, time_stamp):
            """Create a Query object for PostgreSQL using the adapter."""
            print(f"DEBUG: Inside _create_query_postgres for query_id={query_id}", flush=True)
            
            # Normalize table names to lowercase for PostgreSQL compatibility
            def normalize_dict_keys(d):
                return {k.lower(): v for k, v in d.items()}
            
            predicates_normalized = normalize_dict_keys(predicates)
            payloads_normalized = normalize_dict_keys(payloads)
            
            # Create a minimal Query-like object manually
            query = Query.__new__(Query)
            query.id = query_id
            query.predicates = predicates_normalized
            query.payload = payloads_normalized
            query.group_by = {}
            query.order_by = {}
            
            # Use adapter to get selectivity (with normalized keys)
            query.selectivity = self.db.get_selectivity(query_string, predicates_normalized)
            
            query.query_string = query_string
            query.frequency = 1
            query.last_seen_round = time_stamp
            query.first_seen_round = time_stamp
            
            # Initialize scan time structures
            tables = self.db.get_tables()
            query.table_scan_times = {t: [] for t in tables.keys()}
            query.index_scan_times = {t: [] for t in tables.keys()}
            query.table_scan_times_hyp = {t: [] for t in tables.keys()}
            query.index_scan_times_hyp = {t: [] for t in tables.keys()}
            query.context = None
            
            return query

        super_arm_scores = {}
        super_arm_counts = {}
        best_super_arm = set()

        logging.info("Logging configs...\n")
        helper.log_configs(logging, configs)
        logging.info("Logging constants...\n")
        helper.log_configs(logging, constants)
        logging.info("Starting MAB...\n")

        # Get all the columns from the database
        all_columns, number_of_columns = _get_all_columns()
        context_size = number_of_columns * (
                    1 + constants.CONTEXT_UNIQUENESS + constants.CONTEXT_INCLUDES) + constants.STATIC_CONTEXT_SIZE

        # Create oracle and the bandit
        configs.max_memory -= int(_get_current_pds_size())
        oracle = Oracle(configs.max_memory)
        c3ucb_bandit = bandits.C3UCB(context_size, configs.input_alpha, configs.input_lambda, oracle)

        # Running the bandit for T rounds and gather the reward
        arm_selection_count = {}
        chosen_arms_last_round = {}
        next_workload_shift = 0

        # next_workload_shift act as the workload id
        # [query_start, query_end] constitude a workload
        queries_start = configs.queries_start_list[next_workload_shift]
        queries_end = configs.queries_end_list[next_workload_shift]
        query_obj_additions = []

        total_time = 0.0

        for t in range((configs.rounds + configs.hyp_rounds)):
            print(f"DEBUG: Starting round {t}/{configs.rounds + configs.hyp_rounds}", flush=True)
            # e.g., rounds=25, hyp_rounds=0, t as the round iterator
            logging.info(f"round: {t}")
            start_time_round = datetime.datetime.now()
            # At the start of the round we will read the applicable set for the current round.
            # This is a workaround used to demo the dynamic query flow.
            # We read the queries from the start and move the window each round

            # check if workload shift is required
            if t - configs.hyp_rounds == configs.workload_shifts[next_workload_shift]:
                queries_start = configs.queries_start_list[next_workload_shift]
                queries_end = configs.queries_end_list[next_workload_shift]
                if len(configs.workload_shifts) > next_workload_shift + 1:
                    next_workload_shift += 1

            # New set of queries in this batch, required for query execution
            queries_current_batch = self.queries[queries_start:queries_end]

            # Adding new queries to the query store
            query_obj_list_current = []
            for n in range(len(queries_current_batch)):
                # for each query, transform and append to the query_obj_store
                query = queries_current_batch[n]  # a dict of query info
                query_id = query['id']
                if query_id in self.query_obj_store:
                    query_obj_in_store = self.query_obj_store[query_id]
                    query_obj_in_store.frequency += 1
                    query_obj_in_store.last_seen_round = t
                    query_obj_in_store.query_string = query['query_string']
                    if query_obj_in_store.first_seen_round == -1:
                        query_obj_in_store.first_seen_round = t
                else:
                    if self.uses_adapter:
                        # For PostgreSQL: create Query manually and use adapter for selectivity
                        print(f"DEBUG: About to call _create_query_postgres for query_id={query_id}", flush=True)
                        query = _create_query_postgres(query_id, query['query_string'], 
                                                       query['predicates'], query['payload'], t)
                    else:
                        # For MSSQL: use standard Query class
                        query = Query(self.connection, query_id, query['query_string'], query['predicates'],
                                      query['payload'], t)
                    query.context = bandit_helper.get_query_context_v1(query, all_columns, number_of_columns)
                    self.query_obj_store[query_id] = query
                query_obj_list_current.append(self.query_obj_store[query_id])

            # This list contains all past queries, we don't include new queries seen for the first time.
            query_obj_list_past, query_obj_list_new = [], []
            for key, obj in self.query_obj_store.items():
                if t - obj.last_seen_round <= constants.QUERY_MEMORY\
                    and 0 <= obj.first_seen_round < t: # Have seen in previous rounds
                    query_obj_list_past.append(obj)
                elif t - obj.last_seen_round > constants.QUERY_MEMORY: # To be forgotten
                    obj.first_seen_round = -1
                elif obj.first_seen_round == t:  # new seen in the current round
                    query_obj_list_new.append(obj)

            # We don't want to reset in the first round,
            # if there is new additions or removals we identify a workload change
            if t > 0 and len(query_obj_additions) > 0:  # Have seen new query in previous round
                # the number of queries new seen in round t-1 vs. the number of seen queries in rounds 0-(t-1)
                # if the former term > the latter term:
                workload_change = len(query_obj_additions) / len(query_obj_list_past)
                c3ucb_bandit.workload_change_trigger(workload_change)

            # this rounds new will be the additions for the next round
            query_obj_additions = query_obj_list_new

            # Get the predicates for queries and Generate index arms for each query
            index_arms = {}
            for i in range(len(query_obj_list_past)):  # for each previously seen query
                bandit_arms_tmp = bandit_helper.gen_arms_from_predicates_v2(_get_db_connection(), query_obj_list_past[i])
                for key, index_arm in bandit_arms_tmp.items():
                    if key not in index_arms:
                        index_arm.query_ids = set()
                        index_arm.query_ids_backup = set()
                        index_arm.clustered_index_time = 0
                        index_arms[key] = index_arm
                    index_arm.clustered_index_time += max(
                        query_obj_list_past[i].table_scan_times[index_arm.table_name]) if \
                        query_obj_list_past[i].table_scan_times[index_arm.table_name] else 0
                    index_arms[key].query_ids.add(index_arm.query_id)
                    index_arms[key].query_ids_backup.add(index_arm.query_id)

            # set the index arms at the bandit
            if t == configs.hyp_rounds and configs.hyp_rounds != 0:
                index_arms = {}
            index_arm_list = list(index_arms.values())
            logging.info(f"Generated {len(index_arm_list)} arms")
            c3ucb_bandit.set_arms(index_arm_list)

            # creating the context, here we pass all the columns in the database
            context_vectors_v1 = bandit_helper.get_name_encode_context_vectors_v2(index_arms, all_columns,
                                                                                  number_of_columns,
                                                                                  constants.CONTEXT_UNIQUENESS,
                                                                                  constants.CONTEXT_INCLUDES)
            context_vectors_v2 = bandit_helper.get_derived_value_context_vectors_v3(_get_db_connection(), index_arms, query_obj_list_past,
                                                                                        chosen_arms_last_round, not constants.CONTEXT_INCLUDES)
            context_vectors = []
            for i in range(len(context_vectors_v1)):
                context_vectors.append(
                    numpy.array(list(context_vectors_v2[i]) + list(context_vectors_v1[i]),
                                ndmin=2))
            # getting the super arm from the bandit
            chosen_arm_ids = c3ucb_bandit.select_arm_v2(context_vectors, t)
            if t >= configs.hyp_rounds and t - configs.hyp_rounds > constants.STOP_EXPLORATION_ROUND:
                chosen_arm_ids = list(best_super_arm)

            # get objects for the chosen set of arm ids
            chosen_arms = {}
            used_memory = 0
            if chosen_arm_ids:
                chosen_arms = {}
                for arm in chosen_arm_ids:
                    index_name = index_arm_list[arm].index_name
                    chosen_arms[index_name] = index_arm_list[arm]
                    used_memory = used_memory + index_arm_list[arm].memory
                    if index_name in arm_selection_count:
                        arm_selection_count[index_name] += 1
                    else:
                        arm_selection_count[index_name] = 1

            # clean everything at start of actual rounds
            if configs.hyp_rounds != 0 and t == configs.hyp_rounds:
                _bulk_drop_index(chosen_arms_last_round)
                chosen_arms_last_round = {}

            # finding the difference between last round and this round
            keys_last_round = set(chosen_arms_last_round.keys())
            keys_this_round = set(chosen_arms.keys())
            key_intersection = keys_last_round & keys_this_round
            key_additions = keys_this_round - key_intersection
            key_deletions = keys_last_round - key_intersection
            logging.info(f"Selected: {keys_this_round}")
            logging.debug(f"Added: {key_additions}")
            logging.debug(f"Removed: {key_deletions}")

            added_arms = {}
            deleted_arms = {}
            for key in key_additions:
                added_arms[key] = chosen_arms[key]
            for key in key_deletions:
                deleted_arms[key] = chosen_arms_last_round[key]

            start_time_create_query = datetime.datetime.now()
            time_taken, creation_cost_dict, arm_rewards = _create_query_drop(chosen_arms, added_arms, deleted_arms, query_obj_list_current, t)
            end_time_create_query = datetime.datetime.now()
            creation_cost = sum(creation_cost_dict.values())
            if t == configs.hyp_rounds and configs.hyp_rounds != 0:
                # logging arm usage counts
                logging.info("\n\nIndex Usage Counts:\n" + pp.pformat(
                    sorted(arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)))
                arm_selection_count = {}

            c3ucb_bandit.update_v4(chosen_arm_ids, arm_rewards)
            super_arm_id = frozenset(chosen_arm_ids)
            if t >= configs.hyp_rounds:
                if super_arm_id in super_arm_scores:
                    super_arm_scores[super_arm_id] = super_arm_scores[super_arm_id] * super_arm_counts[super_arm_id] \
                                                     + time_taken
                    super_arm_counts[super_arm_id] += 1
                    super_arm_scores[super_arm_id] /= super_arm_counts[super_arm_id]
                else:
                    super_arm_counts[super_arm_id] = 1
                    super_arm_scores[super_arm_id] = time_taken

            # keeping track of queries that we saw last time
            chosen_arms_last_round = chosen_arms

            if t == (configs.rounds + configs.hyp_rounds - 1):
                _bulk_drop_index(chosen_arms)

            end_time_round = datetime.datetime.now()
            current_config_size = float(_get_current_pds_size())
            logging.info("Size taken by the config: " + str(current_config_size) + "MB")
            # Adding information to the results array
            if t >= configs.hyp_rounds:
                actual_round_number = t - configs.hyp_rounds
                recommendation_time = (end_time_round - start_time_round).total_seconds() - (
                            end_time_create_query - start_time_create_query).total_seconds()
                total_round_time = creation_cost + time_taken + recommendation_time
                results.append([actual_round_number, constants.MEASURE_BATCH_TIME, total_round_time])
                results.append([actual_round_number, constants.MEASURE_INDEX_CREATION_COST, creation_cost])
                results.append([actual_round_number, constants.MEASURE_QUERY_EXECUTION_COST, time_taken])
                results.append(
                    [actual_round_number, constants.MEASURE_INDEX_RECOMMENDATION_COST, recommendation_time])
                results.append([actual_round_number, constants.MEASURE_MEMORY_COST, current_config_size])
            else:
                total_round_time = (end_time_round - start_time_round).total_seconds() - (
                        end_time_create_query - start_time_create_query).total_seconds()
                results.append([t, constants.MEASURE_HYP_BATCH_TIME, total_round_time])
            total_time += total_round_time

            if t >= configs.hyp_rounds:
                best_super_arm = min(super_arm_scores, key=super_arm_scores.get)

            print(f"current total {t}: ", total_time)

        logging.info("Time taken by bandit for " + str(configs.rounds) + " rounds: " + str(total_time))
        logging.info("\n\nIndex Usage Counts:\n" + pp.pformat(
            sorted(arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)))
        _restart_server()
        return results, total_time


def parse_args():
    """Parse command line arguments."""
    import argparse
    parser = argparse.ArgumentParser(
        description='C3UCB Bandit Simulator for Database Index Selection',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run with PostgreSQL (default)
  python sim_c3ucb_vR.py
  
  # Run with explicit PostgreSQL
  python sim_c3ucb_vR.py --db-type postgresql
  
  # Run with MSSQL
  python sim_c3ucb_vR.py --db-type mssql
  
  # Custom connection parameters
  python sim_c3ucb_vR.py --db-type postgresql --db-server myhost --db-name tpch
        """
    )
    
    parser.add_argument(
        '--db-type',
        choices=['postgresql', 'postgres', 'mssql', 'sqlserver'],
        default='postgresql',
        help='Database type (default: postgresql)'
    )
    parser.add_argument(
        '--db-server',
        default=None,
        help='Database server hostname (default: from config or localhost)'
    )
    parser.add_argument(
        '--db-name',
        '--database',
        default=None,
        help='Database name (default: from config or tpch)'
    )
    parser.add_argument(
        '--db-user',
        '--username',
        default=None,
        help='Database username (default: from config)'
    )
    parser.add_argument(
        '--db-password',
        '--password',
        default=None,
        help='Database password (default: from config)'
    )
    parser.add_argument(
        '--db-port',
        type=int,
        default=None,
        help='Database port (default: from config or standard port)'
    )
    parser.add_argument(
        '--db-schema',
        '--schema',
        default=None,
        help='Database schema (default: public for PostgreSQL, dbo for MSSQL)'
    )
    parser.add_argument(
        '--experiment',
        default=None,
        help='Override experiment ID from config'
    )
    parser.add_argument(
        '--use-optimizer-costs',
        action='store_true',
        default=True,
        help='Use EXPLAIN optimizer cost estimates instead of actual query execution (default: True for PostgreSQL)'
    )
    parser.add_argument(
        '--use-actual-execution',
        action='store_true',
        default=False,
        help='Use actual query execution instead of optimizer costs (slower but real metrics)'
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    
    # Determine database type
    db_type = args.db_type.lower()
    use_postgres = db_type in ['postgresql', 'postgres']
    
    # Override experiment ID if specified
    if args.experiment:
        configs.experiment_id = args.experiment
        print(f"Using experiment ID: {args.experiment}")
    
    # Create database adapter with command line overrides
    if use_postgres:
        import configparser
        print(f"Using PostgreSQL database (type: {db_type})")
        
        # Read config file to get default values
        config_path = constants.ROOT_DIR + constants.DB_CONFIG
        db_config = configparser.ConfigParser()
        db_config.read(config_path)
        
        # Get default values from config, fallback to PostgreSQL defaults if section missing
        config_db_type = db_config.get('SYSTEM', 'db_type', fallback='MSSQL')
        
        # Build connection parameters with config defaults, allowing command-line overrides
        # Default PostgreSQL connection: Unix socket at /tmp, port 51204, user sclai
        db_params = {
            'db_type': db_type,
            'server': args.db_server or db_config.get('POSTGRESQL', 'server', fallback='/tmp'),
            'database': args.db_name or db_config.get('POSTGRESQL', 'database', fallback='indexselection_tpch___1'),
            'username': args.db_user or db_config.get('POSTGRESQL', 'username', fallback='sclai'),
            'password': args.db_password if args.db_password is not None else db_config.get('POSTGRESQL', 'password', fallback=''),
            'schema': args.db_schema or db_config.get('POSTGRESQL', 'schema', fallback='public'),
            'port': args.db_port or db_config.getint('POSTGRESQL', 'port', fallback=51204),
        }
        
        # Always use create_db_adapter_with_params when use_postgres is True
        from database import create_db_adapter_with_params
        db = create_db_adapter_with_params(**db_params)
        
        db.connect()
        print(f"Connected to {db_type}")
        
        # Enable HypoPG for hypothetical indexes if available
        hypopg_available = False
        if hasattr(db, 'enable_hypopg'):
            try:
                db.enable_hypopg()
                hypopg_available = True
                print("HypoPG enabled for hypothetical indexes")
                # With HypoPG, we can use hyp_rounds > 0
                if configs.hyp_rounds == 0:
                    print("Note: hyp_rounds is 0, but HypoPG supports hypothetical indexes")
            except Exception as e:
                print(f"Warning: Could not enable HypoPG: {e}")
                print("Falling back to real index creation")
        else:
            print("Warning: HypoPG not available, using real index creation")
            # Force hyp_rounds = 0 if no HypoPG
            if configs.hyp_rounds != 0:
                print("Setting hyp_rounds = 0 (no HypoPG support)")
                configs.hyp_rounds = 0
        
        # Auto-detect PostgreSQL workload if using PostgreSQL
        if use_postgres and 'postgresql' not in configs.workload_file.lower():
            pg_workload = configs.workload_file.replace('.json', '_postgresql.json')
            if os.path.exists(pg_workload):
                configs.workload_file = pg_workload
                print(f"Using PostgreSQL workload: {pg_workload}")
        
        # Determine execution mode
        use_optimizer = args.use_optimizer_costs and not args.use_actual_execution
        if use_optimizer:
            print("Using optimizer cost estimation mode (EXPLAIN costs, no actual query execution)")
        else:
            print("Using actual query execution mode (slower but real metrics)")
        
        simulator = Simulator(db_adapter=db, hypopg_available=hypopg_available, use_optimizer_costs=use_optimizer)
    else:
        print(f"Using MSSQL database (legacy mode)")
        simulator = Simulator()
    
    # Running MAB
    print(f"\nRunning experiment: {configs.experiment_id}")
    print(f"Rounds: {configs.rounds}, Reps: {configs.reps}")
    print("-" * 60)
    
    exp_report_mab = ExpReport(configs.experiment_id, constants.COMPONENT_MAB, configs.reps, configs.rounds)
    
    for r in range(configs.reps):
        print(f"\n--- Repetition {r + 1}/{configs.reps} ---")
        
        if not use_postgres:
            simulator = Simulator()  # Recreate for each rep in legacy mode
        
        sim_results, total_workload_time = simulator.run()
        
        temp = DataFrame(sim_results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                               constants.DF_COL_MEASURE_VALUE])
        temp = pd.concat([temp, pd.DataFrame([[-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time]], 
                                              columns=temp.columns)])
        temp[constants.DF_COL_REP] = r
        exp_report_mab.add_data_list(temp)
    
    # Disconnect if using adapter
    if use_postgres and 'db' in locals():
        db.disconnect()
        print("\nDisconnected from database")
    
    print("\n" + "=" * 60)
    print("Generating plots and reports...")
    
    # plot line graphs
    helper.plot_exp_report(configs.experiment_id, [exp_report_mab],
                           (constants.MEASURE_BATCH_TIME, constants.MEASURE_QUERY_EXECUTION_COST))
    
    print(f"✓ Experiment complete! Results in: experiments/{configs.experiment_id}/")
