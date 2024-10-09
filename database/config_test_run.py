import datetime
import logging

import constants
from database.dbconn import DBConnection
from shared import helper


class ConfigRunner:
    @staticmethod
    def run(conf_dict, uniform=False):
        # configuring the logger
        logging.getLogger().setLevel(constants.LOGGING_LEVEL)
        cr_exp_config = conf_dict['exp_conf']
        cr_db_config = conf_dict['db_conf']
        next_workload_shift = 0
        queries_start = cr_exp_config.queries_start_list[next_workload_shift]
        queries_end = cr_exp_config.queries_end_list[next_workload_shift]
        next_config_shift = 0
        connection = DBConnection(cr_db_config)
        queries = helper.get_queries_v2(db_type=connection.db_type)

        # Query execution
        execution_cost = 0.0
        execution_cost_last_config = 0
        apply_cost = 0.0
        start_time_workload = datetime.datetime.now()
        results = []
        for i in range(cr_exp_config.rounds):
            logging.info("Round :" + str(i))
            start_time_round = datetime.datetime.now()
            execution_cost_round = 0
            apply_cost_round = 0

            # check if workload shift is required
            if i == cr_exp_config.workload_shifts[next_workload_shift]:
                queries_start = cr_exp_config.queries_start_list[next_workload_shift]
                queries_end = cr_exp_config.queries_end_list[next_workload_shift]
                if len(cr_exp_config.workload_shifts) > next_workload_shift + 1:
                    next_workload_shift += 1

            # check if config shift is required
            if i == cr_exp_config.config_shifts[next_config_shift]:
                config_start = cr_exp_config.config_start_list[next_config_shift]
                config_end = cr_exp_config.config_end_list[next_config_shift]
                if len(cr_exp_config.config_shifts) > next_config_shift + 1:
                    next_config_shift += 1
                apply_cost_round = ConfigRunner.create_index(config_file_name, config_start, config_end)
                results.append([i, constants.MEASURE_INDEX_CREATION_COST, apply_cost_round])
                execution_cost_last_config = 0

            if uniform and len(cr_exp_config.config_shifts) == 1 and i - cr_exp_config.config_shifts[
             next_config_shift] >= constants.UNIFORM_ASSUMPTION_START:
                cost = execution_cost_last_config / constants.UNIFORM_ASSUMPTION_START
                execution_cost_round += cost
            else:
                for query in queries[queries_start:queries_end]:
                    cost, index_seeks, clustered_index_scans = sql_helper.execute_query_v1(connection,
                                                                                           query['query_string'])
                    logging.info(f"Query {query['id']} cost: {cost}")
                    execution_cost_round += cost
                    execution_cost_last_config += cost

            end_time_round = datetime.datetime.now()
            actual_time_spent_round = end_time_round - start_time_round
            execution_cost += execution_cost_round
            apply_cost += apply_cost_round
            results.append([i, constants.MEASURE_BATCH_TIME, execution_cost_round + apply_cost_round])
            results.append([i, constants.MEASURE_QUERY_EXECUTION_COST, execution_cost_round])
            logging.info("Execution cost: " + str(execution_cost_round))

        connection.close()
        end_time_workload = datetime.datetime.now()
        actual_time_spent = (end_time_workload - start_time_workload).total_seconds()
        logging.info("\texecution cost:" + str(execution_cost) + "s")
        logging.info("Avg cost per round: " + str(execution_cost / cr_exp_config.rounds) + "s")
        # End - Step 3

        total_workload_time = execution_cost + apply_cost
        logging.info("Total workload time: " + str(total_workload_time) + "s")

        # Removing the indexes
        connection.remove_all_non_clustered(connection, constants.SCHEMA_NAME)
        connection.restart_db()
        return results, total_workload_time

    @staticmethod
    def create_index(conf_dict, start, end):
        db_conf_dict = conf_dict['db_conf']
        connection = DBConnection(db_conf_dict)

        # Tuning Parameters
        config_file = db_conf_dict
        # Reading the config from the file
        with open(config_file) as f:
            query_lines = f.readlines()
        joined_string = ''.join(query_lines)
        config_changes = joined_string.split('\n\n\n')

        # Step 2: Implementing the config
        time_apply = 0
        if joined_string != '':
            for creation_query in config_changes[start:end]:
                time_apply += connection.create_index_v2(creation_query)
            logging.info("Time taken to apply the config: " + str(time_apply) + "s")
            logging.info("Size taken by the config: " + str(connection.get_current_pds_size(connection)) + "MB")

        return time_apply
