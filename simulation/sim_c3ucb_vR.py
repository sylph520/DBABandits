import datetime
import logging
import operator
import pprint
import argparse
import pickle
import numpy
from pandas import DataFrame

from shared.configs_v2 import get_exp_config
import shared.helper as helper
import bandits.bandit_c3ucb_v2 as bandits
import bandits.bandit_helper_v2 as bandit_helper
import constants as constants
from bandits.experiment_report import ExpReport
from database.query_v5 import Query
from simulation.base_simulator import BaseSimulator


class Simulator(BaseSimulator):
    def __init__(self, kwargs: dict = ...):
        super().__init__(kwargs)

    def get_round_query_obj_batch_and_update_query_store(self, queries_current_batch, t):
        query_obj_list_current = []
        for n in range(len(queries_current_batch)):  # Adding new queries to the query store
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
                query = Query(self.dbconn, query_id, query['query_string'], query['predicates'],
                              query['payload'], t)
                query.context = bandit_helper.get_query_context_v1(query, self.all_columns, self.number_of_columns)
                self.query_obj_store[query_id] = query
            query_obj_list_current.append(self.query_obj_store[query_id])
        return query_obj_list_current

    @staticmethod
    def update_arm_query_info(q_bandit_arms_tmp: dict, round_index_arms: dict):
        for key, index_arm in q_bandit_arms_tmp.items():  # update the member query info of arms
            if key not in round_index_arms:  # a new arm
                index_arm.query_ids = set()
                index_arm.query_ids_backup = set()
                round_index_arms[key] = index_arm
                # index_arm.clustered_index_time = 0
            # index_arm.clustered_index_time += max(
            #     query_obj_list_past[i].table_scan_time_dict[index_arm.table_name]) if \
            #     query_obj_list_past[i].table_scan_time_dict[index_arm.table_name] else 0
            round_index_arms[key].query_ids.add(index_arm.query_id)
            round_index_arms[key].query_ids_backup.add(index_arm.query_id)

    def run(self):
        pp = pprint.PrettyPrinter()

        results = []
        super_arm_scores = {}
        super_arm_counts = {}
        best_super_arm = set()
        sim_run_total_time = 0.0

        # logging.info("Logging configs...\n")
        # helper.log_configs(logging, self.exp_config)
        # logging.info("Logging constants...\n")
        # helper.log_configs(logging, constants)
        logging.info("Starting MAB...\n")

        # Create oracle and the bandit
        c3ucb_bandit = bandits.C3UCB(self.context_size, self.exp_config.input_alpha, self.exp_config.input_lambda, self.oracle)

        # Running the bandit for T rounds and gather the reward
        round_arm_selection_count = {}
        chosen_arms_last_round = {}
        next_workload_shift = 0

        # next_workload_shift act as the workload id, [query_start, query_end] constitude a workload
        queries_start, queries_end = self.exp_config.queries_start_list[next_workload_shift], self.exp_config.queries_end_list[next_workload_shift]
        query_obj_additions = []

        for t in range((self.exp_config.rounds + self.exp_config.hyp_rounds)):  # loop through the round batch
            # e.g., rounds=25, hyp_rounds=0, t as the round iterator
            logging.info(f"round: {t}")
            round_start_time = datetime.datetime.now()
            # At the start of the round we will read the applicable set for the current round.
            # This is a workaround used to demo the dynamic query flow.
            # We read the queries from the start and move the window each round

            # check if workload shift is required, i.e., the round number reached the set round to shift
            # and set new query batch if needs shifting
            if t - self.exp_config.hyp_rounds == self.exp_config.workload_shifts[next_workload_shift]:
                queries_start = self.exp_config.queries_start_list[next_workload_shift]
                queries_end = self.exp_config.queries_end_list[next_workload_shift]
                if len(self.exp_config.workload_shifts) > next_workload_shift + 1:
                    next_workload_shift += 1

            # New set of queries in this batch, required for query execution
            queries_current_batch = self.queries[queries_start:queries_end]
            query_obj_list_current = self.get_round_query_obj_batch_and_update_query_store(queries_current_batch, t)

            # This list contains all past queries, we don't include new queries seen for the first time.
            query_obj_list_past, query_obj_list_new = [], []
            for key, obj in self.query_obj_store.items():
                if t - obj.last_seen_round <= constants.QUERY_MEMORY\
                        and 0 <= obj.first_seen_round < t:  # Have seen in previous rounds
                    query_obj_list_past.append(obj)
                elif t - obj.last_seen_round > constants.QUERY_MEMORY:  # To be forgotten
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
            round_index_arms = {}
            # TODO: whether only considering arms for previously seen queries reasonable
            if t == self.exp_config.hyp_rounds and self.exp_config.hyp_rounds != 0:
                round_index_arms = {}
            else:
                for i in range(len(query_obj_list_past)):  # for each previously seen query
                    q_bandit_arms_tmp = bandit_helper.gen_arms_from_predicates_v2(
                        self.dbconn, query_obj_list_past[i])
                    self.update_arm_query_info(q_bandit_arms_tmp, round_index_arms)

            index_arm_list = list(round_index_arms.values())
            logging.info(f"Generated {len(index_arm_list)} arms")
            c3ucb_bandit.set_arms(index_arm_list)

            # creating the context, here we pass all the columns in the database
            context_vectors_v1 = bandit_helper.get_name_encode_context_vectors_v2(round_index_arms, self.all_columns,
                                                                                  self.number_of_columns,
                                                                                  constants.CONTEXT_UNIQUENESS,
                                                                                  constants.CONTEXT_INCLUDES)
            context_vectors_v2 = bandit_helper.get_derived_value_context_vectors_v3(self.dbconn, round_index_arms, query_obj_list_past,
                                                                                    chosen_arms_last_round, not constants.CONTEXT_INCLUDES)
            context_vectors = []
            for i in range(len(context_vectors_v1)):
                context_vectors.append(
                    numpy.array(list(context_vectors_v2[i]) + list(context_vectors_v1[i]),
                                ndmin=2))

            # getting the super arm from the bandit
            if t >= self.exp_config.hyp_rounds and t - self.exp_config.hyp_rounds > constants.STOP_EXPLORATION_ROUND:
                chosen_arm_ids = list(best_super_arm)
            else:
                chosen_arm_ids = c3ucb_bandit.select_arm_v2(context_vectors, t)

            # get objects for the chosen set of arm ids
            chosen_arms = {}
            used_memory = 0
            if chosen_arm_ids:
                chosen_arms = {}
                for arm in chosen_arm_ids:
                    index_name = index_arm_list[arm].index_name
                    chosen_arms[index_name] = index_arm_list[arm]
                    used_memory = used_memory + index_arm_list[arm].memory
                    if index_name in round_arm_selection_count:
                        round_arm_selection_count[index_name] += 1
                    else:
                        round_arm_selection_count[index_name] = 1

            # clean everything at start of actual rounds
            if self.exp_config.hyp_rounds != 0 and t == self.exp_config.hyp_rounds:
                self.dbconn.bulk_drop_index(constants.SCHEMA_NAME, chosen_arms_last_round)
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
            # arm_rewards: tuple (gains, creation cost) reward got form playing each arm
            if t < self.exp_config.hyp_rounds:
                time_taken, creation_cost_dict, arm_rewards = self.dbconn.hyp_create_query_drop_v2(constants.SCHEMA_NAME,
                                                                                                   chosen_arms, added_arms, deleted_arms,
                                                                                                   query_obj_list_current)
            else:
                time_taken, creation_cost_dict, arm_rewards = self.dbconn.create_query_drop_v3(constants.SCHEMA_NAME,
                                                                                               chosen_arms, added_arms,
                                                                                               deleted_arms,
                                                                                               query_obj_list_current)
            end_time_create_query = datetime.datetime.now()
            idx_creation_cost = sum(creation_cost_dict.values())

            if t == self.exp_config.hyp_rounds and self.exp_config.hyp_rounds != 0:
                # logging arm usage counts
                logging.info("\n\nIndex Usage Counts:\n" + pp.pformat(
                    sorted(round_arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)))
                round_arm_selection_count = {}

            c3ucb_bandit.update_v4(chosen_arm_ids, arm_rewards)

            super_arm_id = frozenset(chosen_arm_ids)
            if t >= self.exp_config.hyp_rounds:
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

            round_end_time = datetime.datetime.now()
            current_config_size = float(self.dbconn.get_current_pds_size())
            logging.info("Size taken by the config: " + str(current_config_size) + "MB")

            if t == (self.exp_config.rounds + self.exp_config.hyp_rounds - 1):
                self.dbconn.bulk_drop_index(constants.SCHEMA_NAME, chosen_arms)

            # Adding information to the results array
            if t >= self.exp_config.hyp_rounds:
                actual_round_number = t - self.exp_config.hyp_rounds
                recommendation_time = (round_end_time - round_start_time).total_seconds() - (
                    end_time_create_query - start_time_create_query).total_seconds()
                round_total_time = idx_creation_cost + time_taken + recommendation_time
                results.append([actual_round_number, constants.MEASURE_BATCH_TIME, round_total_time])
                results.append([actual_round_number, constants.MEASURE_INDEX_CREATION_COST, idx_creation_cost])
                results.append([actual_round_number, constants.MEASURE_QUERY_EXECUTION_COST, time_taken])
                results.append([actual_round_number, constants.MEASURE_INDEX_RECOMMENDATION_COST, recommendation_time])
                results.append([actual_round_number, constants.MEASURE_MEMORY_COST, current_config_size])
            else:
                round_total_time = (round_end_time - round_start_time).total_seconds() - (
                    end_time_create_query - start_time_create_query).total_seconds()
                results.append([t, constants.MEASURE_HYP_BATCH_TIME, round_total_time])
            sim_run_total_time += round_total_time

            if t >= self.exp_config.hyp_rounds:
                best_super_arm = min(super_arm_scores, key=super_arm_scores.get)

            print(f"current total {t}: ", sim_run_total_time)

        logging.info("Time taken by bandit for " + str(self.exp_config.rounds) + " rounds: " + str(sim_run_total_time))
        logging.info("\n\nIndex Usage Counts:\n" + pp.pformat(
            sorted(round_arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)))
        # self.connection.restart_db()
        return results, sim_run_total_time


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--exp_id', type=str, default='tpc_h_static_10_MAB')
    parser.add_argument('--db_type', type=str, default='postgresql')
    args = parser.parse_args()

    exp_id = args.exp_id
    db_type = args.db_type

    FROM_FILE = False
    SEPARATE_EXPERIMENTS = True
    PLOT_LOG_Y = False
    # PLOT_MEASURE = (constants.MEASURE_BATCH_TIME, constants.MEASURE_QUERY_EXECUTION_COST,
    #                 constants.MEASURE_INDEX_CREATION_COST)
    plot_measure = (constants.MEASURE_BATCH_TIME, constants.MEASURE_QUERY_EXECUTION_COST,
                    constants.MEASURE_INDEX_CREATION_COST, constants.MEASURE_MEMORY_COST,
                    constants.MEASURE_TOTAL_WORKLOAD_TIME)
    UNIFORM = False

    if SEPARATE_EXPERIMENTS:
        exp_report_list = []
    local_exp_config = get_exp_config(exp_id=exp_id)
    database_name = local_exp_config.database
    conf_dict = {
        "db_conf": {
            "db_type": db_type,
            "database": database_name},
        "exp_conf": local_exp_config
    }
    experiment_folder_path = helper.get_experiment_folder_path(exp_id)
    if FROM_FILE:
        with open(experiment_folder_path + "reports.pickle", "rb") as f:
            exp_report_list = exp_report_list + pickle.load(f)
    else:
        logging.basicConfig(
            filename=experiment_folder_path + exp_id + '.log',
            filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
        logging.getLogger().setLevel(constants.LOGGING_LEVEL)
        print("Currently running: ", exp_id)
        version_number = 'R'
        exp_report_mab = ExpReport(local_exp_config.experiment_id, constants.COMPONENT_MAB + version_number + exp_id,
                                   local_exp_config.reps, local_exp_config.rounds)

        # Running MAB
        for r in range(local_exp_config.reps):
            simulator = Simulator(conf_dict)
            sim_results, total_workload_time = simulator.run()
            temp = DataFrame(sim_results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                   constants.DF_COL_MEASURE_VALUE])
            temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
            temp[constants.DF_COL_REP] = r
            exp_report_mab.add_data_list(temp)

    # plot line graphs
    helper.plot_exp_report(local_exp_config.experiment_id, [exp_report_mab], plot_measure)
