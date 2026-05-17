import sys
import os

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
from database import DatabaseInterface

# Force unbuffered output
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 1)

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

print("DEBUG: Script starting...", flush=True)

# opencode: NEW FUNCTION - Auto-detect debugger
def _is_debugging():
    """Auto-detect if running in debugger (VSCode, PyCharm, etc.)"""
    import sys
    import os

    if sys.flags.debug:
        return True

    if sys.gettrace() is not None:
        return True

    debug_env_vars = [
        'VSCODE_DEBUGGING', 'PYTHONDEBUGPY', 'DEBUGPY',
        'PYDEVD', 'PYCHARM_DEBUG'
    ]
    for var in debug_env_vars:
        if os.environ.get(var):
            return True

    return False

# opencode: Helper function for dictionary key normalization
def normalize_dict_keys(d):
    """Normalize dictionary keys to lowercase for PostgreSQL compatibility."""
    return {k.lower(): v for k, v in d.items()}

# Simulation built on vQ to collect the super arm performance
# Now supports both MSSQL and PostgreSQL via database abstraction layer

# opencode: NEW CLASS - Base simulator with database adapter support
class BaseSimulator:
    # opencode: NEW METHOD - Initialize simulator with adapter support
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
        # opencode: NEW ATTRIBUTE - HypoPG availability flag
        self.hypopg_available = hypopg_available
        # opencode: NEW ATTRIBUTE - Use optimizer costs mode
        self.use_optimizer_costs = use_optimizer_costs
        # opencode: Unit for cost values: 's' for MSSQL/actual execution, 'cost_units' for PostgreSQL planner
        self.cost_unit = 's'

        # Get the query List
        self.queries = helper.get_queries_v2()

        # opencode: MODIFIED - Support both adapter and legacy connection
        if db_adapter is not None:
            self.db = db_adapter
            self.uses_adapter = True
            if not self.db._connection:
                self.db.connect()
            self.connection = self.db
        else:
            # Legacy MSSQL mode
            self.connection = sql_connection.get_sql_connection()
            self.db = None
            self.uses_adapter = False

        self.query_obj_store: Dict[int, Query] = {}
        reload(bandit_helper)

# opencode: NEW CLASS - Simulator with class methods for refactoring
class Simulator(BaseSimulator):
    # Simulator inherit from BaseSimulator (init queries and db connections)
    def run(self):
        pp = pprint.PrettyPrinter()
        # Note: configs are reloaded in main() before creating Simulator
        # Do not reload here to preserve CLI overrides

        # Check if using PostgreSQL without HypoPG and warn about hyp_rounds
        if self.uses_adapter and configs.hyp_rounds != 0 and not self.hypopg_available:
            logging.warning("PostgreSQL without HypoPG detected - setting hyp_rounds to 0")
            print("⚠️  WARNING: hyp_rounds set to 0 (HypoPG not available)")
            configs.hyp_rounds = 0

        results = []

        super_arm_scores = {}
        super_arm_counts = {}
        best_super_arm = set()

        logging.info("Logging configs...\n")
        helper.log_configs(logging, configs)
        logging.info("Logging constants...\n")
        helper.log_configs(logging, constants)
        logging.info("Starting MAB...\n")

        # Get all the columns from the database
        all_columns, number_of_columns = self.get_all_columns()
        context_size = number_of_columns * (
                    1 + constants.CONTEXT_UNIQUENESS + constants.CONTEXT_INCLUDES) + constants.STATIC_CONTEXT_SIZE

        # Create oracle and the bandit
        configs.max_memory -= int(self.get_current_pds_size())
        oracle = Oracle(configs.max_memory)
        c3ucb_bandit = bandits.C3UCB(context_size, configs.input_alpha, configs.input_lambda, oracle)
        c3ucb_bandit.set_enable_cluster_filter(configs.enable_cluster_filter)
        c3ucb_bandit.set_enable_query_overlap_filter(configs.enable_query_overlap_filter)

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
                        print(f"DEBUG: About to call create_query_postgres for query_id={query_id}", flush=True)
                        query = self.create_query_postgres(query_id, query['query_string'], 
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

            logging.debug(f"Round {t}: new queries={len(query_obj_list_new)}, past queries={len(query_obj_list_past)}")

            # We don't want to reset in the first round,
            # if there is new additions or removals we identify a workload change
            if t > 0 and len(query_obj_additions) > 0:  # Have seen new query in previous round
                # the number of queries new seen in round t-1 vs. the number of seen queries in rounds 0-(t-1)
                # if the former term > the latter term:
                workload_change = len(query_obj_additions) / len(query_obj_list_past)
                c3ucb_bandit.workload_change_trigger(workload_change)

            # this rounds new will be the additions for the next round
            query_obj_additions = query_obj_list_new

            # Skip arm generation if no past queries (first round or no history)
            if len(query_obj_list_past) == 0:
                logging.info(f"Round {t}: No past queries ({len(query_obj_list_new)} new), skipping arm selection")
                # Still execute queries but skip index recommendations
                index_arm_list = []
                chosen_arm_ids = []
                chosen_arms = {}
            else:
                # Get the predicates for queries and Generate index arms for each query
                index_arms = {}
                for i in range(len(query_obj_list_past)):  # for each previously seen query
                    bandit_arms_tmp = bandit_helper.gen_arms_from_predicates_v2(self.get_db_connection(), query_obj_list_past[i])
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
                # logging.info(f": {[a.index_name for a in index_arm_list]}")
                c3ucb_bandit.set_arms(index_arm_list)

                # creating the context, here we pass all the columns in the database
                context_vectors_v1 = bandit_helper.get_name_encode_context_vectors_v2(index_arms, all_columns,
                                                                              number_of_columns,
                                                                              constants.CONTEXT_UNIQUENESS,
                                                                              constants.CONTEXT_INCLUDES)
                context_vectors_v2 = bandit_helper.get_derived_value_context_vectors_v3(self.get_db_connection(), index_arms, query_obj_list_past,
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
                    self.bulk_drop_index(chosen_arms_last_round)
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
            time_taken, creation_cost_dict, arm_rewards = self.create_query_drop(chosen_arms, added_arms, deleted_arms, query_obj_list_current, t)
            end_time_create_query = datetime.datetime.now()
            creation_cost = sum(creation_cost_dict.values())
            index_config = {
                'selected': list(keys_this_round),
                'added': list(key_additions),
                'deleted': list(key_deletions),
                'total_memory_mb': sum(a.memory for a in chosen_arms.values()) if chosen_arms else 0
            }
            logging.info(f"Round {t}: execute_cost={time_taken:.2f} {self.cost_unit}, creation_cost={creation_cost:.2f} {self.cost_unit}, "
                         f"indexes={index_config}, rewards={arm_rewards}")
            if t == configs.hyp_rounds and configs.hyp_rounds != 0:
                # logging arm usage counts
                logging.info("\n\nIndex Usage Counts:\n" + pp.pformat(
                    sorted(arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)))
                arm_selection_count = {}

            # Skip bandit update if no arms were selected
            if len(chosen_arm_ids) > 0:
                c3ucb_bandit.update_v4(chosen_arm_ids, arm_rewards)
                logging.debug(f"Round {t}: arm_rewards={arm_rewards}, chosen_arm_ids={chosen_arm_ids}")
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
                self.bulk_drop_index(chosen_arms)

            end_time_round = datetime.datetime.now()
            current_config_size = float(self.get_current_pds_size())
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

            if t >= configs.hyp_rounds and super_arm_scores:
                best_super_arm = min(super_arm_scores, key=super_arm_scores.get)

            print(f"current total {t}: ", total_time)

        logging.info("Time taken by bandit for " + str(configs.rounds) + " rounds: " + str(total_time))
        logging.info("\n\nIndex Usage Counts:\n" + pp.pformat(
            sorted(arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)))
        self.restart_server()
        return results, total_time

    # === Class methods for database operations (opencode: refactored from inner functions) ===

    # opencode: NEW METHOD - Get database connection
    def get_db_connection(self):
        """Get database connection - adapter or legacy."""
        if self.uses_adapter:
            return self.db
        return self.connection

    # opencode: NEW METHOD - Get all columns
    def get_all_columns(self):
        """Get all columns - adapter or legacy."""
        if self.uses_adapter:
            return self.db.get_all_columns()
        return sql_helper.get_all_columns(self.connection)

    # opencode: NEW METHOD - Get current PDS size
    def get_current_pds_size(self):
        """Get current PDS size - adapter or legacy."""
        if self.uses_adapter:
            return self.db.get_current_pds_size()
        return sql_helper.get_current_pds_size(self.connection)

    # opencode: NEW METHOD - Create query and drop
    def create_query_drop(self, chosen_arms, added_arms, deleted_arms, queries, t):
        """Create indexes, execute queries, drop indexes - adapter or legacy."""
        if self.uses_adapter:
            # Set hypopg_enabled based on current round and use_real_indexes_in_rounds flag
            # - In hyp_rounds phase: always use hypothetical (True)
            # - In rounds phase: use hypothetical if use_real_indexes_in_rounds is False
            if hasattr(self.db, 'hypopg_enabled'):
                if t < configs.hyp_rounds:
                    self.db.hypopg_enabled = True
                else:
                    self.db.hypopg_enabled = not getattr(configs, 'use_real_indexes_in_rounds', False)

            for index_name, bandit_arm in deleted_arms.items():
                self.db.drop_index(bandit_arm.table_name, bandit_arm.index_name)

            creation_cost = {}
            for index_name, bandit_arm in added_arms.items():
                cost = self.db.create_index(
                    bandit_arm.table_name,
                    bandit_arm.index_cols,
                    bandit_arm.index_name,
                    bandit_arm.include_cols
                )
                creation_cost[index_name] = cost

            # opencode: Phase for reward calculation
            # - Optimizer mode: always simple reward (hypothetical execution)
            # - Actual mode: simple in hyp_rounds, complex in rounds
            is_hyp_phase = t < configs.hyp_rounds
            is_hyp_phase_for_reward = self.use_optimizer_costs or is_hyp_phase

            # opencode: Baseline attribute based on use_optimizer_costs (aligned with master)
            # - use_optimizer_costs=True: use table_scan_times_hyp (optimizer estimates)
            # - use_optimizer_costs=False: use table_scan_times (actual execution)
            baseline_attr = 'table_scan_times_hyp' if self.use_optimizer_costs else 'table_scan_times'

            # opencode: Initialize accumulators
            execute_cost = 0
            arm_rewards = {}

            for query in queries:
                # opencode: Use ANALYZE only if NOT using optimizer costs
                # use_optimizer_costs=True -> use EXPLAIN only (fast, estimates)
                # use_optimizer_costs=False -> use EXPLAIN ANALYZE (actual execution)
                use_analyze = not self.use_optimizer_costs

                # Get index usage based on mode
                if self.use_optimizer_costs:
                    # Optimizer mode: use EXPLAIN (use_analyze=False by default)
                    plan_info = self.db.get_query_plan(query.query_string, use_analyze=use_analyze)
                    execute_cost += plan_info.est_statement_sub_tree_cost
                    index_usage = plan_info.non_clustered_index_usage
                    total_cost = plan_info.est_statement_sub_tree_cost
                    # opencode: Extract clustered scan data for penalty in real phase
                    clustered_scans = {}
                    for c in plan_info.clustered_index_usage:
                        clustered_scans[c.table_name] = c.elapsed_time
                else:
                    # Actual execution mode: execute queries and measure real time
                    time_taken, nc_usage, c_usage = self.db.execute_query(query.query_string, clear_cache=False)
                    execute_cost += time_taken
                    index_usage = [(idx_use.index_name, idx_use.table_name, idx_use.elapsed_time) for idx_use in nc_usage]
                    total_cost = time_taken
                    # opencode: Extract clustered scan data for penalty
                    clustered_scans = {}
                    for c in c_usage:
                        clustered_scans[c.table_name] = c.elapsed_time

                # opencode: Calculate rewards with marginal contribution
                query_arm_rewards, used_any = self._calculate_arm_rewards_marginal(
                    query, index_usage, chosen_arms, baseline_attr, total_cost,
                    is_hyp_phase, clustered_scans)

                # Merge query rewards into cumulative rewards
                for idx_name, reward in query_arm_rewards.items():
                    if idx_name not in arm_rewards:
                        arm_rewards[idx_name] = [0, 0]
                    arm_rewards[idx_name][0] += reward

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

    # opencode: NEW METHOD - Marginal contribution reward calculation
    def _calculate_arm_rewards_marginal(self, query, index_usage, chosen_arms, baseline_attr, total_cost, is_hyp_phase, clustered_scans=None):
        """
        opencode: Calculate arm rewards using marginal contribution.

        For each chosen arm, measures the cost reduction when that arm is present vs absent.
        This preserves Option A's root-cost stability while giving per-arm differentiation.

        Fixes applied:
        - P1: Log-transform marginal rewards to handle 10+ order-of-magnitude scale differences
        - P2: Cap marginal contribution at 3x average to prevent "winner takes all" dynamics
        - P3: Zero-marginal arms get a small baseline-based reward instead of 0

        Args:
            query: Query object with root_plan_cost_baseline attribute
            index_usage: List of tuples [(index_name, table_name, idx_cost), ...]
            chosen_arms: Dict of chosen arms {index_name: arm}
            baseline_attr: Attribute name for baseline tracking
            total_cost: Root plan Total Cost with all arms present
            is_hyp_phase: If True (during hyp_rounds), use simple reward (no marginal calc)
            clustered_scans: Dict of {table_name: clustered_scan_cost}

        Returns:
            Tuple of (arm_rewards dict, used_any bool)
        """
        arm_rewards = {}
        used_any = False

        # opencode: Get baseline root plan cost for this query
        baseline_list = getattr(query, 'root_plan_cost_baseline', [])

        if baseline_list:
            baseline_root = max(baseline_list)
        else:
            baseline_root = total_cost

        # opencode: Update baseline with current root cost
        if len(baseline_list) < constants.TABLE_SCAN_TIME_LENGTH:
            baseline_list.append(total_cost)

        if not chosen_arms:
            return arm_rewards, used_any

        # opencode: In hyp_rounds phase or no baseline, use equal distribution (marginal needs stable baseline)
        if is_hyp_phase or not baseline_list:
            used_any = True
            query_reward = baseline_root - total_cost
            per_arm = query_reward / len(chosen_arms)
            for idx_name in chosen_arms.keys():
                arm_rewards[idx_name] = per_arm
            return arm_rewards, used_any

        # opencode: Marginal contribution calculation
        # For each arm, temporarily drop it and measure cost difference
        cost_with_all = total_cost
        marginal_rewards = {}

        for arm_name, arm in chosen_arms.items():
            # Temporarily drop this arm
            self.db.drop_index(arm.table_name, arm_name)

            # Get cost without this arm
            try:
                plan_without = self.db.get_query_plan(query.query_string, use_analyze=False)
                cost_without = plan_without.est_statement_sub_tree_cost
            except Exception as e:
                logging.warning(f"Failed to get plan without {arm_name}: {e}")
                cost_without = cost_with_all
            finally:
                # Re-create the arm
                self.db.create_index(arm.table_name, arm.index_cols, arm_name, arm.include_cols)

            # Marginal contribution = cost without arm - cost with all arms
            # Positive means the arm helped reduce cost
            marginal = cost_without - cost_with_all
            marginal_rewards[arm_name] = marginal

        # opencode: P1 - Apply log-transform to stabilize reward scale
        # Raw marginals span 10+ orders of magnitude (e.g., 7 to 187 billion)
        # Log-transform compresses this range while preserving ordering
        positive_marginals = [m for m in marginal_rewards.values() if m > 0]
        if positive_marginals:
            log_marginals = {}
            for arm_name, marginal in marginal_rewards.items():
                if marginal > 0:
                    # log1p ensures small positive values get meaningful scores
                    log_marginals[arm_name] = numpy.log1p(marginal)
                else:
                    log_marginals[arm_name] = 0.0

            # opencode: P2 - Cap log-marginal at 3x average to prevent "winner takes all"
            avg_log = numpy.mean([v for v in log_marginals.values() if v > 0]) if any(v > 0 for v in log_marginals.values()) else 1.0
            cap = 3.0 * avg_log
            for arm_name in log_marginals:
                if log_marginals[arm_name] > cap:
                    log_marginals[arm_name] = cap

            # opencode: Distribute baseline improvement proportionally to log-transformed marginals
            total_log = sum(v for v in log_marginals.values() if v > 0)
            if total_log > 0:
                used_any = True
                query_reward = baseline_root - cost_with_all
                for arm_name, log_marginal in log_marginals.items():
                    if log_marginal > 0:
                        # Proportional share based on log-transformed marginal contribution
                        arm_rewards[arm_name] = query_reward * (log_marginal / total_log)
                    else:
                        # opencode: P3 - Zero-marginal arms get a small baseline-based reward
                        # Instead of 0, give them a tiny fraction to keep them in contention
                        arm_rewards[arm_name] = query_reward * 0.01 / max(len(chosen_arms), 1)
            else:
                # Fallback: no positive marginals, use equal distribution
                used_any = True
                query_reward = baseline_root - cost_with_all
                per_arm = query_reward / len(chosen_arms)
                for idx_name in chosen_arms.keys():
                    arm_rewards[idx_name] = per_arm
        else:
            # Fallback: no positive marginals, use equal distribution
            used_any = True
            query_reward = baseline_root - cost_with_all
            per_arm = query_reward / len(chosen_arms)
            for idx_name in chosen_arms.keys():
                arm_rewards[idx_name] = per_arm

        return arm_rewards, used_any

    # opencode: DEPRECATED METHOD - Kept for backward compatibility
    def _calculate_arm_rewards(self, query, index_usage, chosen_arms, baseline_attr, total_cost, is_hyp_phase_for_reward, clustered_scans=None):
        """
        opencode: Calculate arm rewards from root plan cost comparison.
        DEPRECATED: Use _calculate_arm_rewards_marginal instead.

        Uses the root plan's Total Cost as the baseline instead of per-index costs.
        This avoids PostgreSQL's cumulative cost semantics where Index Scan Total Cost
        includes loop multiplication and varies with plan structure.

        Args:
            query: Query object with root_plan_cost_baseline attribute
            index_usage: List of tuples [(index_name, table_name, idx_cost), ...] (unused in Option A)
            chosen_arms: Dict of chosen arms {index_name: arm}
            baseline_attr: Attribute name for baseline tracking
            total_cost: Root plan Total Cost for this query
            is_hyp_phase_for_reward: If True, use simple reward
            clustered_scans: Dict of {table_name: clustered_scan_cost} (unused in Option A)

        Returns:
            Tuple of (arm_rewards dict, used_any bool)
        """
        arm_rewards = {}
        used_any = False

        # opencode: Get baseline root plan cost for this query
        baseline_list = getattr(query, 'root_plan_cost_baseline', [])

        if baseline_list:
            baseline_root = max(baseline_list)
            query_reward = baseline_root - total_cost
        else:
            # First time seeing this query - use negative cost as reward
            query_reward = -1 * total_cost

        # opencode: Update baseline with current root cost
        if len(baseline_list) < constants.TABLE_SCAN_TIME_LENGTH:
            baseline_list.append(total_cost)

        # opencode: Distribute reward equally among chosen arms
        if chosen_arms:
            used_any = True
            per_arm = query_reward / len(chosen_arms)
            for idx_name in chosen_arms.keys():
                arm_rewards[idx_name] = per_arm

        return arm_rewards, used_any

    # opencode: NEW METHOD - Bulk drop index
    def bulk_drop_index(self, bandit_arms):
        """Drop multiple indexes - adapter or legacy."""
        if self.uses_adapter:
            for index_name, bandit_arm in bandit_arms.items():
                self.db.drop_index(bandit_arm.table_name, bandit_arm.index_name)
        else:
            sql_helper.bulk_drop_index(self.connection, constants.SCHEMA_NAME, bandit_arms)

    # opencode: NEW METHOD - Restart server
    def restart_server(self):
        """Restart server - adapter or legacy."""
        if self.uses_adapter and hasattr(self, 'use_optimizer_costs') and not self.use_optimizer_costs:
            self.db.restart_server()
        elif self.uses_adapter:
            logging.info("Skipping server restart (optimizer cost mode - no state to clear)")
        else:
            sql_helper.restart_sql_server()

    # opencode: NEW METHOD - Create query for PostgreSQL
    def create_query_postgres(self, query_id, query_string, predicates, payloads, time_stamp):
        print(f"DEBUG: Inside _create_query_postgres for query_id={query_id}", flush=True)

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
        # opencode: Root plan cost baseline for stable reward calculation (PostgreSQL)
        query.root_plan_cost_baseline = []
        query.context = None

        return query

# opencode: Helper functions for generate_experiment_config_name()
def sanitize_config_value(s):
    """Sanitize a string value for use in config folder names."""
    if s is None:
        return "none"
    s = str(s)
    s = s.replace('/', '_').replace('\\', '_').replace(':', '_')
    s = s.replace('__', '_')
    return s

def get_index_mode(rounds, hyp_rounds, use_real_indexes):
    """Determine the exploration mode string based on parameters."""
    if hyp_rounds == 0:
        exploration = "no_hyp_explore"
    else:
        exploration = f"hyp_explore_{hyp_rounds}"

    if use_real_indexes:
        exploration += "_real"

    return exploration

# opencode: NEW FUNCTION - Generate experiment config folder name
def generate_experiment_config_name(experiment_id, db_type, rounds, hyp_rounds, reps, alpha, lambda_param, workload_file, use_real_indexes=False):
    """
    Generate a unique config folder name based on experiment parameters.

    Format: <experiment_id>__<mode>__rounds-<N>__reps-<N>__alpha-<X>__lambda-<Y>__workload-<name>__db-<type>

    :param experiment_id: base experiment ID from config
    :param db_type: database type (postgresql, mssql)
    :param rounds: number of rounds
    :param hyp_rounds: number of hypothetical rounds
    :param reps: number of repetitions
    :param alpha: C3UCB alpha parameter
    :param lambda_param: C3UCB lambda parameter
    :param workload_file: workload file path
    :param use_real_indexes: whether to use real indexes in rounds phase
    :return: sanitized config folder name
    """
    import os

    workload_name = sanitize_config_value(os.path.basename(workload_file).replace('.json', ''))
    mode = get_index_mode(rounds, hyp_rounds, use_real_indexes)
    db = sanitize_config_value(db_type)

    config_name = f"{experiment_id}__{mode}__rounds-{rounds}__reps-{reps}__alpha-{alpha}__lambda-{lambda_param}__workload-{workload_name}__db-{db}"
    return config_name

# opencode: NEW FUNCTION - Generate timestamp for experiment run
def get_run_timestamp():
    """Generate a timestamp for the experiment run. Format: YYYYMMDD_HHMMSS"""
    from datetime import datetime
    return datetime.now().strftime("%Y%m%d_%H%M%S")

# opencode: NEW FUNCTION - Parse command line arguments
def parse_args():
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

  # Override experiment parameters from command line
  python sim_c3ucb_vR.py --rounds 10 --reps 2 --hyp-rounds 5 --max-memory 50000

  # Use actual query execution (slower but real metrics)
  python sim_c3ucb_vR.py --no-optimizer-costs

  # Custom bandit parameters
  python sim_c3ucb_vR.py --alpha 2.0 --lambda 0.3

  # Use specific workload file
  python sim_c3ucb_vR.py --workload /resources/workloads/tpc_h_static_100_postgresql.json
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
        '--no-optimizer-costs',
        action='store_true',
        default=False,
        help='Use actual query execution instead of optimizer costs (slower but real metrics)'
    )
    parser.add_argument(
        '--hyp-rounds',
        type=int,
        default=None,
        help='Number of hypothetical rounds (HypoPG rounds). Overrides config. Default: from exp.conf'
    )
    parser.add_argument(
        '--use-real-indexes',
        action='store_true',
        default=False,
        help='Use real indexes for rounds phase (instead of hypothetical). Default: uses hypothetical'
    )
    parser.add_argument(
        '--rounds',
        type=int,
        default=None,
        help='Number of actual rounds. Overrides config. Default: from exp.conf'
    )
    parser.add_argument(
        '--reps',
        type=int,
        default=None,
        help='Number of repetitions. Overrides config. Default: from exp.conf'
    )
    parser.add_argument(
        '--workload',
        default=None,
        help='Workload file path. Overrides config. Default: from exp.conf'
    )
    parser.add_argument(
        '--alpha',
        type=float,
        default=None,
        help='Alpha parameter for C3UCB. Overrides config. Default: from exp.conf'
    )
    parser.add_argument(
        '--lambda',
        dest='lambda_param',
        type=float,
        default=None,
        help='Lambda parameter for C3UCB. Overrides config. Default: from exp.conf'
    )
    parser.add_argument(
        '--max-memory',
        type=int,
        default=None,
        help='Maximum memory for indexes (MB). Overrides config. Default: from exp.conf'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable debug logging (useful when running in debugger)'
    )
    parser.add_argument(
        '--log-level',
        type=str,
        default=None,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Set logging level (overrides -v if provided)'
    )
    parser.add_argument(
        '--no-cluster-filter',
        action='store_true',
        default=False,
        help='Disable cluster-based arm filtering in oracle (allows all covering indexes to compete)'
    )
    parser.add_argument(
        '--no-query-overlap-filter',
        action='store_true',
        default=False,
        help='Disable query_id overlap filtering in oracle (allows redundant partial indexes)'
    )
    parser.add_argument(
        '--hyp-cost-mode',
        type=str,
        default='none',
        choices=['none', 'size', 'fixed'],
        help='Creation cost mode for hypothetical indexes: none (0.00), size-based, or fixed'
    )
    parser.add_argument(
        '--hyp-cost-fixed',
        type=float,
        default=0.01,
        help='Fixed creation cost (seconds per MB) when --hyp-cost-mode=fixed'
    )
    parser.add_argument(
        '--hyp-cost-size-multiplier',
        type=float,
        default=0.001,
        help='Size multiplier (seconds per MB) when --hyp-cost-mode=size'
    )
    parser.add_argument(
        '--include-hyp-cost',
        action='store_true',
        default=False,
        help='Include estimated creation cost for hypothetical indexes (uses --hyp-cost-mode)'
    )

    parser.add_argument(
        '--no-file-log',
        action='store_true',
        dest='no_file_log',
        help='Disable log file, plots, and tables - console only output (for quick debugging)'
    )
    parser.add_argument(
        '--minimal',
        action='store_true',
        dest='no_file_log',  # Same as --no-file-log
        help='Minimal mode: disable all file output (alias for --no-file-log)'
    )

    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()

    # Determine database type
    db_type = args.db_type.lower()
    use_postgres = db_type in ['postgresql', 'postgres']

    # Reload configs first, then override with CLI arguments
    reload(configs)

    # Override experiment ID if specified
    if args.experiment:
        configs.experiment_id = args.experiment
        print(f"Using experiment ID: {args.experiment}")

    # Override config values with CLI arguments (applied after reload)
    if args.hyp_rounds is not None:
        configs.hyp_rounds = args.hyp_rounds
        print(f"Using hyp_rounds from CLI: {args.hyp_rounds}")

    if args.rounds is not None:
        configs.rounds = args.rounds
        print(f"Using rounds from CLI: {args.rounds}")

    # Apply --use-real-indexes flag (default: False, meaning use hypothetical in rounds phase)
    configs.use_real_indexes_in_rounds = args.use_real_indexes
    if configs.use_real_indexes_in_rounds:
        print("Using --use-real-indexes: Rounds phase will use real indexes")
    else:
        print("Rounds phase will use hypothetical indexes (default)")

    if args.reps is not None:
        configs.reps = args.reps
        print(f"Using reps from CLI: {args.reps}")

    if args.workload is not None:
        configs.workload_file = args.workload
        print(f"Using workload from CLI: {args.workload}")

    if args.alpha is not None:
        configs.input_alpha = args.alpha
        print(f"Using alpha from CLI: {args.alpha}")

    if args.lambda_param is not None:
        configs.input_lambda = args.lambda_param
        print(f"Using lambda from CLI: {args.lambda_param}")

    if args.max_memory is not None:
        configs.max_memory = args.max_memory
        print(f"Using max_memory from CLI: {args.max_memory}")

    # Apply --no-cluster-filter flag
    if args.no_cluster_filter:
        configs.enable_cluster_filter = False
        print("Cluster-based arm filtering disabled")
    else:
        print("Cluster-based arm filtering enabled (default)")

    # Apply --no-query-overlap-filter flag
    if args.no_query_overlap_filter:
        configs.enable_query_overlap_filter = False
        print("Query overlap filtering disabled")
    else:
        print(f"Query overlap filtering enabled (default)")

    # Apply --hyp-cost-mode flag
    if args.hyp_cost_mode:
        configs.hyp_cost_mode = args.hyp_cost_mode
        print(f"Hypo index creation cost mode: {configs.hyp_cost_mode}")
        if args.hyp_cost_mode != 'none':
            if args.hyp_cost_mode == 'fixed':
                configs.hyp_cost_fixed = args.hyp_cost_fixed
                print(f"  Fixed cost: {configs.hyp_cost_fixed} s/MB")
            elif args.hyp_cost_mode == 'size':
                configs.hyp_cost_size_multiplier = args.hyp_cost_size_multiplier
                print(f"  Size multiplier: {configs.hyp_cost_size_multiplier} s/MB")
    else:
        print(f"Hypo index creation cost mode: none (default)")

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
            'use_real_indexes_in_rounds': configs.use_real_indexes_in_rounds,
            'hyp_cost_mode': configs.hyp_cost_mode,
            'hyp_cost_fixed': configs.hyp_cost_fixed,
            'hyp_cost_size_multiplier': configs.hyp_cost_size_multiplier,
            'include_hyp_cost': args.include_hyp_cost,
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

        # Determine execution mode (--no-optimizer-costs means use real execution)
        use_optimizer = not args.no_optimizer_costs
        if use_optimizer:
            print("Using optimizer cost estimation mode (EXPLAIN costs, no actual query execution)")
        else:
            print("Using actual query execution mode (slower but real metrics)")

        # If using real execution, ensure hyp_rounds doesn't conflict
        if not use_optimizer and configs.hyp_rounds > 0:
            print(f"Note: hyp_rounds={configs.hyp_rounds} will be applied (HypoPG for exploration)")

        simulator = Simulator(db_adapter=db, hypopg_available=hypopg_available, use_optimizer_costs=use_optimizer)
        if use_optimizer:
            simulator.cost_unit = 'cost_units'  # PostgreSQL planner cost units
    else:
        print("Using MSSQL database (legacy mode)")
        simulator = Simulator()

    # Generate experiment config name and timestamp for output folder structure
    config_folder_name = generate_experiment_config_name(
        experiment_id=configs.experiment_id,
        db_type=db_type,
        rounds=configs.rounds,
        hyp_rounds=configs.hyp_rounds,
        reps=configs.reps,
        alpha=configs.input_alpha,
        lambda_param=configs.input_lambda,
        workload_file=configs.workload_file,
        use_real_indexes=configs.use_real_indexes_in_rounds
)

    run_timestamp = get_run_timestamp()

    # Create config and run folders - only if NOT minimal mode
    if not args.no_file_log:
        config_folder = helper.get_config_folder_path(config_folder_name)
        print(f"Config folder: {config_folder}")
        run_folder = helper.get_experiment_folder_path(config_folder_name, run_timestamp)
        print(f"Run folder: {run_folder}")
    else:
        config_folder = None
        run_folder = None

    # Update logging to use new path (both file and console output)
    root = logging.getLogger()
    root.handlers = []

    # File handler (optional, disabled with --no-file-log / --minimal)
    if not args.no_file_log:
        fh = logging.FileHandler(run_folder + configs.experiment_id + '.log', mode='w')
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        root.addHandler(fh)

    # Console handler (always enabled)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    root.addHandler(sh)

    # Determine log level: --log-level > -v > auto-detect > default (INFO)
    if args.log_level:
        log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    elif args.verbose or _is_debugging():
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO
    logging.getLogger().setLevel(log_level)
    logging.info(f"Experiment config: {config_folder_name}")
    logging.info(f"CLI args: {vars(args)}")

    # Running MAB
    print(f"\nRunning experiment: {configs.experiment_id}")
    print(f"Config: {config_folder_name}")
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

    # Generate plots and reports - only if NOT minimal mode
    if not args.no_file_log:
        print("Generating plots and reports...")

        # plot line graphs with timestamp and config folder name
        # opencode: log_y=True because cost scales vary by orders of magnitude
        helper.plot_exp_report(
            configs.experiment_id, [exp_report_mab],
            (constants.MEASURE_BATCH_TIME, constants.MEASURE_QUERY_EXECUTION_COST),
            log_y=True,
            timestamp=run_timestamp,
            config_folder_name=config_folder_name
        )

        # create comparison table
        helper.create_comparison_tables(
            configs.experiment_id, [exp_report_mab],
            timestamp=run_timestamp,
            config_folder_name=config_folder_name
        )

        print(f"✓ Experiment complete! Results in: {run_folder}")
    else:
        print("✓ Experiment complete! (minimal mode - no output files)")
