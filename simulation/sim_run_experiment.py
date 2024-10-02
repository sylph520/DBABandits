import pickle
import logging
import argparse
from typing import Dict

from pandas import DataFrame
import pandas as pd

import constants
from bandits.experiment_report import ExpReport
from database.config_test_run import ConfigRunner
# from database.dta_test_run_v2 import DTARunner

from shared.configs_v2 import get_exp_config
from shared import helper

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--exp_ids', nargs='+', default=['tpc_h_static_10_MAB'], help="a list of exp_id")
    parser.add_argument('--db_type', type=str, default='MSSQL')
    args = parser.parse_args()

    exp_id_list = args.exp_ids
    db_type = args.db_type

    # Generate form saved reports
    FROM_FILE = False
    SEPARATE_EXPERIMENTS = True
    PLOT_LOG_Y = False
    PLOT_MEASURE = (constants.MEASURE_BATCH_TIME, constants.MEASURE_QUERY_EXECUTION_COST,
                    constants.MEASURE_INDEX_CREATION_COST)
    UNIFORM = False

    exp_report_list = []
    for i in range(len(exp_id_list)):
        if SEPARATE_EXPERIMENTS:
            exp_report_list = []
        exp_id = exp_id_list[i]
        local_exp_config = get_exp_config(exp_id=exp_id)
        database = local_exp_config.database
        conf_dict = {
            "db_conf": {
                "db_type": db_type,
                "database": database},
            "exp_conf": local_exp_config
        }
        # Comparing components
        OPTIMAL = constants.COMPONENT_OPTIMAL in local_exp_config.components
        TA_OPTIMAL = constants.COMPONENT_TA_OPTIMAL in local_exp_config.components
        TA_FULL = constants.COMPONENT_TA_FULL in local_exp_config.components
        TA_CURRENT = constants.COMPONENT_TA_CURRENT in local_exp_config.components
        TA_SCHEDULE = constants.COMPONENT_TA_SCHEDULE in local_exp_config.components
        MAB = constants.COMPONENT_MAB in local_exp_config.components
        NO_INDEX = constants.COMPONENT_NO_INDEX in local_exp_config.components
        RL = constants.COMPONENT_DDQN in local_exp_config.components
        DDQN = constants.COMPONENT_DDQN in local_exp_config.components

        # set the path for experiment by the exp_id, create the folder if not exists
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
            # Running MAB
            if MAB:
                simulators = {}
                # import the corresponding Simulator classes
                for mab_version in local_exp_config.mab_versions:
                    simulators[mab_version] = (
                        getattr(__import__(mab_version, fromlist=['Simulator']), 'Simulator'))

                # run simulators
                for version, simulator_class in simulators.items():
                    version_number = version.split("_v", 1)[1]
                    exp_report_mab = ExpReport(local_exp_config.experiment_id, constants.COMPONENT_MAB + version_number + exp_id_list[i],
                                               local_exp_config.reps, local_exp_config.rounds)

                    for r in range(local_exp_config.reps):
                        simulator = simulator_class(conf_dict)
                        results, total_workload_time = simulator.run()

                        temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                           constants.DF_COL_MEASURE_VALUE])
                        new_row = pd.DataFrame([[-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time]], columns=temp.columns)
                        temp = pd.concat([temp, new_row])
                        # temp.append(
                        #     [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                        temp[constants.DF_COL_REP] = r
                        exp_report_mab.add_data_list(temp)
                    exp_report_list.append(exp_report_mab)

            # Running No Index
            if NO_INDEX:
                exp_report_no_index = ExpReport(local_exp_config.experiment_id, constants.COMPONENT_NO_INDEX + exp_id_list[i], local_exp_config.reps,
                                                local_exp_config.rounds)
                for r in range(local_exp_config.reps):
                    results, total_workload_time = ConfigRunner.run(conf_dict, uniform=UNIFORM)
                    temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                       constants.DF_COL_MEASURE_VALUE])
                    temp.append(
                        [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                    temp[constants.DF_COL_REP] = r
                    exp_report_no_index.add_data_list(temp)
                exp_report_list.append(exp_report_no_index)
    #
    #         # Running Optimal
    #         if OPTIMAL:
    #             exp_report_optimal = ExpReport(
    #                 local_exp_config.experiment_id, constants.COMPONENT_OPTIMAL + exp_id_list[i], local_exp_config.reps, local_exp_config.rounds)
    #             for r in range(local_exp_config.reps):
    #                 results, total_workload_time = ConfigRunner.run(
    #                     "optimal_config.sql", uniform=UNIFORM)
    #                 temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
    #                                                    constants.DF_COL_MEASURE_VALUE])
    #                 temp.append(
    #                     [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
    #                 temp[constants.DF_COL_REP] = r
    #                 exp_report_optimal.add_data_list(temp)
    #             exp_report_list.append(exp_report_optimal)
    #
    #         # Running DTA Optimal
    #         if TA_OPTIMAL:
    #             exp_report_ta = ExpReport(
    #                 local_exp_config.experiment_id, constants.COMPONENT_TA_OPTIMAL + exp_id_list[i], local_exp_config.reps, local_exp_config.rounds)
    #             for r in range(local_exp_config.reps):
    #                 dta_runner = DTARunner(
    #                     local_exp_config.ta_runs, workload_type=constants.TA_WORKLOAD_TYPE_OPTIMAL)
    #                 results, total_workload_time = dta_runner.run()
    #                 temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
    #                                                    constants.DF_COL_MEASURE_VALUE])
    #                 temp.append(
    #                     [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
    #                 temp[constants.DF_COL_REP] = r
    #                 exp_report_ta.add_data_list(temp)
    #             exp_report_list.append(exp_report_ta)
    #
    #         # Running DTA Full
    #         if TA_FULL:
    #             exp_report_ta = ExpReport(local_exp_config.experiment_id, constants.COMPONENT_TA_FULL + exp_id_list[i], local_exp_config.reps,
    #                                       local_exp_config.rounds)
    #             for r in range(local_exp_config.reps):
    #                 dta_runner = DTARunner(
    #                     [0], workload_type=constants.TA_WORKLOAD_TYPE_FULL)
    #                 results, total_workload_time = dta_runner.run()
    #                 temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
    #                                                    constants.DF_COL_MEASURE_VALUE])
    #                 temp.append(
    #                     [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
    #                 temp[constants.DF_COL_REP] = r
    #                 exp_report_ta.add_data_list(temp)
    #             exp_report_list.append(exp_report_ta)
    #
    #         # Running DTA Current
    #         if TA_CURRENT:
    #             exp_report_ta = ExpReport(local_exp_config.experiment_id, constants.COMPONENT_TA_CURRENT + exp_id_list[i],
    #                                       local_exp_config.reps, local_exp_config.rounds)
    #             for r in range(local_exp_config.reps):
    #                 dta_runner = DTARunner(
    #                     local_exp_config.ta_runs, workload_type=constants.TA_WORKLOAD_TYPE_CURRENT)
    #                 results, total_workload_time = dta_runner.run()
    #                 temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
    #                                                    constants.DF_COL_MEASURE_VALUE])
    #                 temp.append(
    #                     [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
    #                 temp[constants.DF_COL_REP] = r
    #                 exp_report_ta.add_data_list(temp)
    #             exp_report_list.append(exp_report_ta)
    #
    #         # Running DTA Schedule (everything from last run)
    #         if TA_SCHEDULE:
    #             exp_report_ta = ExpReport(local_exp_config.experiment_id, constants.COMPONENT_TA_SCHEDULE + exp_id_list[i],
    #                                       local_exp_config.reps, local_exp_config.rounds)
    #             for r in range(local_exp_config.reps):
    #                 dta_runner = DTARunner(
    #                     local_exp_config.ta_runs, workload_type=constants.TA_WORKLOAD_TYPE_SCHEDULE)
    #                 results, total_workload_time = dta_runner.run()
    #                 temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
    #                                                    constants.DF_COL_MEASURE_VALUE])
    #                 temp.append(
    #                     [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
    #                 temp[constants.DF_COL_REP] = r
    #                 exp_report_ta.add_data_list(temp)
    #             exp_report_list.append(exp_report_ta)
    #
    #         # Running DDQN
    #         if DDQN:
    #             from simulation.sim_ddqn_v3 import Simulator as DDQNSimulator
    #             exp_report_mab = ExpReport(local_exp_config.experiment_id, constants.COMPONENT_MAB + exp_id_list[i],
    #                                        local_exp_config.reps, local_exp_config.rounds)
    #             for r in range(local_exp_config.reps):
    #                 simulator = DDQNSimulator()
    #                 results, total_workload_time = simulator.run()
    #                 temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
    #                                                    constants.DF_COL_MEASURE_VALUE])
    #                 temp.append(
    #                     [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
    #                 temp[constants.DF_COL_REP] = r
    #                 exp_report_mab.add_data_list(temp)
    #             exp_report_list.append(exp_report_mab)
    #
    #         # Save results
    #         with open(experiment_folder_path + "reports.pickle", "wb") as f:
    #             pickle.dump(exp_report_list, f)
    #
    #         if SEPARATE_EXPERIMENTS:
    #             helper.plot_exp_report(local_exp_config.experiment_id,
    #                                    exp_report_list, PLOT_MEASURE, PLOT_LOG_Y)
    #             helper.create_comparison_tables(
    #                 local_exp_config.experiment_id, exp_report_list)
    #
    # # plot line graphs
    # if not SEPARATE_EXPERIMENTS:
    #     helper.plot_exp_report(local_exp_config.experiment_id,
    #                            exp_report_list, PLOT_MEASURE, PLOT_LOG_Y)
    #     helper.create_comparison_tables(local_exp_config.experiment_id, exp_report_list)
