import datetime
import logging
import operator
import pprint
from collections import defaultdict
import argparse
import numpy
from pandas import DataFrame

import bandits.rl_ddqn_v2 as bandits
import bandits.bandit_helper_v2 as bandit_helper
import constants as constants
import shared.helper as helper
from bandits.experiment_report import ExpReport
from bandits.oracle_v2 import OracleV6 as Oracle
from database.query_v5 import Query
from simulation.base_simulator import BaseSimulator


# Simulation built on vO to work on dynamic workloads
class Simulator(BaseSimulator):

    def run(self):
        pp = pprint.PrettyPrinter()
        # start_time_workload = datetime.datetime.now()
        results = []
        # logging.info("Logging configs...\n")
        # helper.log_configs(logging, self.exp_config)
        # logging.info("Logging constants...\n")
        # helper.log_configs(logging, constants)
        # logging.info("Starting DDQN...\n")

        # setup_time_start = datetime.datetime.now()
        # Create oracle and the bandit
        c3ucb_bandit = bandits.DDQN(self.context_size, self.oracle)

        # Running the bandit for T rounds and gather the reward
        arm_selection_count = {}
        time_taken_hist = []
        used_memory_hist = []
        chosen_arms_last_round = {}
        next_workload_shift = 0
        queries_start = self.exp_config.queries_start_list[next_workload_shift]
        queries_end = self.exp_config.queries_end_list[next_workload_shift]
        query_obj_additions = []
        # setup_time_end = datetime.datetime.now()
        # setup_time = (setup_time_end - setup_time_start).total_seconds()
        total_time = 0.0

        for t in range((self.exp_config.rounds + self.exp_config.hyp_rounds)):
            logging.info(f"round: {t}")
            round_start_time = datetime.datetime.now()
            # At the start of the round we will read the applicable set for the current round. This is a workaround
            # used to demo the dynamic query flow. We read the queries from the start and move the window each round

            # check if workload shift is required
            if t - self.exp_config.hyp_rounds == self.exp_config.workload_shifts[next_workload_shift]:
                queries_start = self.exp_config.queries_start_list[next_workload_shift]
                queries_end = self.exp_config.queries_end_list[next_workload_shift]
                if len(self.exp_config.workload_shifts) > next_workload_shift + 1:
                    next_workload_shift += 1

            # New set of queries in this batch, required for query execution
            queries_current_batch = self.query_jsons[queries_start:queries_end]

            # Adding new queries to the query store
            query_obj_list_current = []
            for n in range(len(queries_current_batch)):
                query = queries_current_batch[n]
                query_id = query['id']
                if query_id in self.query_obj_store:
                    query_obj_in_store = self.query_obj_store[query_id]
                    query_obj_in_store.frequency += 1
                    query_obj_in_store.last_seen = t
                    query_obj_in_store.query_string = query['query_string']
                    if query_obj_in_store.first_seen == -1:
                        query_obj_in_store.first_seen = t
                else:
                    query = Query(self.dbconn, query_id, query['query_string'], query['predicates'],
                                  query['payload'], t)
                    query.context = bandit_helper.get_query_context_v1(query, self.all_columns, self.number_of_columns)
                    self.query_obj_store[query_id] = query
                query_obj_list_current.append(self.query_obj_store[query_id])

            # This list contains all past queries, we don't include new queries seen for the first time.
            query_obj_list_past = []
            query_obj_list_new = []
            for key, obj in self.query_obj_store.items():
                if t - obj.last_seen <= constants.QUERY_MEMORY and 0 <= obj.first_seen < t:
                    query_obj_list_past.append(obj)
                elif t - obj.last_seen > constants.QUERY_MEMORY:
                    obj.first_seen = -1
                elif obj.first_seen == t:
                    query_obj_list_new.append(obj)

            # We don't want to reset in the first round, if there is new additions or removals we identify a
            # workload change
            if t > 0 and len(query_obj_additions) > 0:
                workload_change = len(query_obj_additions) / len(query_obj_list_past)
                # c3ucb_bandit.workload_change_trigger(workload_change)

            # this rounds new will be the additions for the next round
            query_obj_additions = query_obj_list_new

            # Get the predicates for queries and Generate index arms for each query
            index_arms = {}
            for i in range(len(query_obj_list_past)):
                bandit_arms_tmp = bandit_helper.gen_arms_from_predicates_v2(self.dbconn, query_obj_list_past[i])
                for key, index_arm in bandit_arms_tmp.items():
                    if key not in index_arms:
                        index_arm.query_ids = set()
                        index_arm.query_ids_backup = set()
                        index_arms[key] = index_arm
                    index_arms[key].query_ids.add(index_arm.query_id)
                    index_arms[key].query_ids_backup.add(index_arm.query_id)

            # set the index arms at the bandit
            if t == self.exp_config.hyp_rounds and self.exp_config.hyp_rounds != 0:
                index_arms = {}
            index_arm_list = list(index_arms.values())
            logging.info(f"Generated {len(index_arm_list)} arms")
            # c3ucb_bandit.set_arms(index_arm_list)

            # creating the context, here we pass all the columns in the database
            context_vectors_v1 = bandit_helper.get_name_encode_context_vectors_v2(index_arms, self.all_columns,
                                                                                  self.number_of_columns,
                                                                                  constants.CONTEXT_UNIQUENESS,
                                                                                  constants.CONTEXT_INCLUDES)
            context_vectors_v2 = bandit_helper.get_derived_value_context_vectors_v2(self.dbconn, index_arms,
                                                                                    query_obj_list_past,
                                                                                    chosen_arms_last_round,
                                                                                    not constants.CONTEXT_INCLUDES)
            context_vectors = []
            for i in range(len(context_vectors_v1)):
                context_vectors.append(
                    numpy.array(list(context_vectors_v2[i]) + list(context_vectors_v1[i]),
                                ndmin=2))
            # getting the super arm from the bandit
            c3ucb_bandit.set_arms(index_arm_list)
            if t == 1:
                c3ucb_bandit.init_agents(context_vectors)
            if t > 0:
                chosen_arm_ids = c3ucb_bandit.select_arm(context_vectors, t)
            else:
                chosen_arm_ids = []

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
            if self.exp_config.hyp_rounds != 0 and t == self.exp_config.hyp_rounds:
                self.dbconn.bulk_drop_index(self.dbconn, constants.SCHEMA_NAME, chosen_arms_last_round)
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
            if t < self.exp_config.hyp_rounds:
                time_taken, creation_cost_dict, arm_rewards = self.dbconn.hyp_create_query_drop_v1(self.dbconn, constants.SCHEMA_NAME,
                                                                                                       chosen_arms, added_arms, deleted_arms,
                                                                                                       query_obj_list_current)
            else:
                time_taken, creation_cost_dict, arm_rewards = self.dbconn.create_query_drop_v2(self.dbconn,
                                                                                                   constants.SCHEMA_NAME,
                                                                                                   chosen_arms, added_arms,
                                                                                                   deleted_arms,
                                                                                                   query_obj_list_current)
            end_time_create_query = datetime.datetime.now()
            creation_cost = sum(creation_cost_dict.values())
            if t == self.exp_config.hyp_rounds and self.exp_config.hyp_rounds != 0:
                hyp_statistic_dict = defaultdict(list)
                hyp_statistic_dict['Optimizer cost'] = time_taken_hist.copy()
                helper.plot_moving_average(hyp_statistic_dict, constants.WINDOW_SIZE,
                                           f'Optimizer Cost Over {self.exp_config.hyp_rounds} Rounds',
                                           self.exp_config.experiment_id)
                # logging arm usage counts
                logging.info("\n\nIndex Usage Counts:\n" + pp.pformat(
                    sorted(arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)))
                time_taken_hist = []
                used_memory_hist = []
                arm_selection_count = {}

            time_taken_hist.append(time_taken)
            used_memory_hist.append(used_memory)
            c3ucb_bandit.update(chosen_arm_ids, arm_rewards)

            # keeping track of queries that we saw last time
            chosen_arms_last_round = chosen_arms

            if t == (self.exp_config.rounds + self.exp_config.hyp_rounds - 1):
                self.dbconn.bulk_drop_index(self.dbconn, constants.SCHEMA_NAME, chosen_arms)

            round_end_time = datetime.datetime.now()
            # Adding information to the results array
            if t >= self.exp_config.hyp_rounds:
                actual_round_number = t - self.exp_config.hyp_rounds
                recommendation_time = (round_end_time - round_start_time).total_seconds() - (
                    end_time_create_query - start_time_create_query).total_seconds()
                total_round_time = creation_cost + time_taken + recommendation_time
                results.append([actual_round_number, constants.MEASURE_BATCH_TIME, total_round_time])
                results.append([actual_round_number, constants.MEASURE_INDEX_CREATION_COST, creation_cost])
                results.append([actual_round_number, constants.MEASURE_QUERY_EXECUTION_COST, time_taken])
                results.append(
                    [actual_round_number, constants.MEASURE_INDEX_RECOMMENDATION_COST, recommendation_time])
                results.append([actual_round_number, constants.MEASURE_MEMORY_COST, used_memory])
            else:
                total_round_time = (round_end_time - round_start_time).total_seconds()
                results.append([t, constants.MEASURE_HYP_BATCH_TIME, total_round_time])
            total_time += total_round_time

        # end_time_workload = datetime.datetime.now()
        # logging arm usage counts and time spent
        # total_time = (end_time_workload - start_time_workload).total_seconds()
        logging.info("Time taken by DDQN for " + str(self.exp_config.rounds) + " rounds: " + str(total_time))
        logging.info("\n\nIndex Usage Counts:\n" + pp.pformat(
            sorted(arm_selection_count.items(), key=operator.itemgetter(1), reverse=True)))

        return results, total_time


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--exp_config', type=str)
    args = parser.parse_args()
    exp_report_mab = ExpReport(args.exp_config.experiment_id, constants.COMPONENT_MAB,
            args.exp_config.reps, args.exp_config.rounds)
    for r in range(args.exp_config.reps):
        simulator = Simulator()
        results, total_workload_time = simulator.run()
        temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                           constants.DF_COL_MEASURE_VALUE])
        temp.append([-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
        temp[constants.DF_COL_REP] = r
        exp_report_mab.add_data_list(temp)

    # plot line graphs
    helper.plot_exp_report(args.exp_config.experiment_id, [exp_report_mab], constants.MEASURE_BATCH_TIME)
