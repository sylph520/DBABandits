import datetime
import logging
import operator
import pprint
from importlib import reload
from typing import Dict

import numpy
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


# Simulation built on vQ to collect the super arm performance


class BaseSimulator:
    def __init__(self):
        """
        setup queries (self.queries), db connection (self.connection),
        and an empty query_object_store
        """
        # configuring the logger
        logging.basicConfig(
            filename=helper.get_experiment_folder_path(configs.experiment_id) + configs.experiment_id + '.log',
            filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
        logging.getLogger().setLevel(logging.INFO)

        # Get the query List
        self.queries = helper.get_queries_v2()
        self.connection = sql_connection.get_sql_connection()
        self.query_obj_store: Dict[int, Query] = {}
        reload(bandit_helper)


class Simulator(BaseSimulator):
    # Simulator inherit from BaseSimulator (init queries and db connections)
    def run(self):
        pp = pprint.PrettyPrinter()
        reload(configs)

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
        all_columns, number_of_columns = sql_helper.get_all_columns(self.connection)
        context_size = number_of_columns * (
                    1 + constants.CONTEXT_UNIQUENESS + constants.CONTEXT_INCLUDES) + constants.STATIC_CONTEXT_SIZE

        # Create oracle and the bandit
        configs.max_memory -= int(sql_helper.get_current_pds_size(self.connection))
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
                bandit_arms_tmp = bandit_helper.gen_arms_from_predicates_v2(self.connection, query_obj_list_past[i])
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
            context_vectors_v2 = bandit_helper.get_derived_value_context_vectors_v3(self.connection, index_arms, query_obj_list_past,
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
                sql_helper.bulk_drop_index(self.connection, constants.SCHEMA_NAME, chosen_arms_last_round)
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
            if t < configs.hyp_rounds:
                time_taken, creation_cost_dict, arm_rewards = sql_helper.hyp_create_query_drop_v2(self.connection, constants.SCHEMA_NAME,
                                                                                                  chosen_arms, added_arms, deleted_arms,
                                                                                                  query_obj_list_current)
            else:
                time_taken, creation_cost_dict, arm_rewards = sql_helper.create_query_drop_v3(self.connection,
                                                                                              constants.SCHEMA_NAME,
                                                                                              chosen_arms, added_arms,
                                                                                              deleted_arms,
                                                                                              query_obj_list_current)
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
                sql_helper.bulk_drop_index(self.connection, constants.SCHEMA_NAME, chosen_arms)

            end_time_round = datetime.datetime.now()
            current_config_size = float(sql_helper.get_current_pds_size(self.connection))
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
        sql_helper.restart_sql_server()
        return results, total_time


if __name__ == "__main__":
    # Running MAB
    exp_report_mab = ExpReport(configs.experiment_id, constants.COMPONENT_MAB, configs.reps, configs.rounds)
    for r in range(configs.reps):
        simulator = Simulator()
        sim_results, total_workload_time = simulator.run()
        temp = DataFrame(sim_results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                               constants.DF_COL_MEASURE_VALUE])
        temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
        temp[constants.DF_COL_REP] = r
        exp_report_mab.add_data_list(temp)

    # plot line graphs
    helper.plot_exp_report(configs.experiment_id, [exp_report_mab],
                           (constants.MEASURE_BATCH_TIME, constants.MEASURE_QUERY_EXECUTION_COST))
