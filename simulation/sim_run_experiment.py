import argparse
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pickle
from importlib import reload
import logging

from pandas import DataFrame
import pandas as pd

import constants
from bandits.experiment_report import ExpReport
from bandits.registry import get_bandit
from database.config_test_run import ConfigRunner
from database.dta_test_run_v2 import DTARunner
from database import create_db_adapter_with_params, DatabaseInterface
from shared import configs_v2 as configs, helper
import configparser


def parse_args():
    parser = argparse.ArgumentParser(description='Run MAB experiment simulations')
    parser.add_argument('--db-type', type=str, default='postgresql',
                        choices=['postgresql', 'postgres', 'mssql', 'sqlserver'],
                        help='Database type (default: postgresql)')
    parser.add_argument('--db-server', type=str, default=None,
                        help='Database server host')
    parser.add_argument('--db-name', type=str, default=None,
                        help='Database name')
    parser.add_argument('--db-user', type=str, default=None,
                        help='Database user')
    parser.add_argument('--db-password', type=str, default=None,
                        help='Database password')
    parser.add_argument('--db-port', type=int, default=None,
                        help='Database port')
    parser.add_argument('--db-schema', type=str, default=None,
                        help='Database schema')
    return parser.parse_args()


def main():
    args = parse_args()

    # Determine if using PostgreSQL
    use_postgres = args.db_type in ['postgresql', 'postgres']
    db_adapter = None

    # Create db_adapter if using PostgreSQL
    if use_postgres:
        print("Using PostgreSQL database adapter")
        
        # Read config file to get default values
        config_path = constants.ROOT_DIR + constants.DB_CONFIG
        db_config = configparser.ConfigParser()
        db_config.read(config_path)
        
        # Build connection parameters with config defaults, allowing command-line overrides
        # Default PostgreSQL connection: Unix socket at /tmp, port 51204, user sclai
        db_params = {
            'db_type': args.db_type,
            'server': args.db_server or db_config.get('POSTGRESQL', 'server', fallback='/tmp'),
            'database': args.db_name or db_config.get('POSTGRESQL', 'database', fallback='indexselection_tpch___1'),
            'username': args.db_user or db_config.get('POSTGRESQL', 'username', fallback='sclai'),
            'password': args.db_password if args.db_password is not None else db_config.get('POSTGRESQL', 'password', fallback=''),
            'schema': args.db_schema or db_config.get('POSTGRESQL', 'schema', fallback='public'),
            'port': args.db_port or db_config.getint('POSTGRESQL', 'port', fallback=51204),
        }
        
        db_adapter = create_db_adapter_with_params(**db_params)
        db_adapter.connect()

    # Define Experiment ID list that we need to run
    exp_id_list = ["tpc_h_skew_static_10_MAB3"]
    # exp_id_list = ["tpc_h_static_10_MAB"]  # lsc, uniform static

    # Comparing components
    OPTIMAL = constants.COMPONENT_OPTIMAL in configs.components
    TA_OPTIMAL = constants.COMPONENT_TA_OPTIMAL in configs.components
    TA_FULL = constants.COMPONENT_TA_FULL in configs.components
    TA_CURRENT = constants.COMPONENT_TA_CURRENT in configs.components
    TA_SCHEDULE = constants.COMPONENT_TA_SCHEDULE in configs.components
    MAB = constants.COMPONENT_MAB in configs.components
    NO_INDEX = constants.COMPONENT_NO_INDEX in configs.components
    RL = constants.COMPONENT_DDQN in configs.components

    # Generate form saved reports
    FROM_FILE = False
    SEPARATE_EXPERIMENTS = True
    PLOT_LOG_Y = False
    PLOT_MEASURE = (constants.MEASURE_BATCH_TIME, constants.MEASURE_QUERY_EXECUTION_COST,
                    constants.MEASURE_INDEX_CREATION_COST)
    UNIFORM = False

    exp_report_list = []

    try:
        for i in range(len(exp_id_list)):
            if SEPARATE_EXPERIMENTS:
                exp_report_list = []
            experiment_folder_path = helper.get_experiment_folder_path(exp_id_list[i])
            helper.change_experiment(exp_id_list[i])
            reload(configs)
            reload(logging)

            OPTIMAL = constants.COMPONENT_OPTIMAL in configs.components
            TA_OPTIMAL = constants.COMPONENT_TA_OPTIMAL in configs.components
            TA_FULL = constants.COMPONENT_TA_FULL in configs.components
            TA_CURRENT = constants.COMPONENT_TA_CURRENT in configs.components
            TA_SCHEDULE = constants.COMPONENT_TA_SCHEDULE in configs.components
            MAB = constants.COMPONENT_MAB in configs.components
            NO_INDEX = constants.COMPONENT_NO_INDEX in configs.components
            DDQN = constants.COMPONENT_DDQN in configs.components

            # configuring the logger
            if not FROM_FILE:
                logging.basicConfig(
                    filename=experiment_folder_path + configs.experiment_id + '.log',
                    filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
                logging.getLogger().setLevel(constants.LOGGING_LEVEL)

            if FROM_FILE:
                with open(experiment_folder_path + "reports.pickle", "rb") as f:
                    exp_report_list = exp_report_list + pickle.load(f)
            else:
                print("Currently running: ", exp_id_list[i])

                # Running MAB
                if MAB:
                    Simulators = {}
                    for mab_version in configs.mab_versions:
                        Simulators[mab_version] = get_bandit(mab_version)
                    for version, Simulator in Simulators.items():
                        version_number = version.split("_v", 1)[1] if "_v" in version else ""
                        exp_report_mab = ExpReport(configs.experiment_id,
                                                   constants.COMPONENT_MAB + version_number +
                                                   exp_id_list[i], configs.reps,
                                                   configs.rounds)
                        for r in range(configs.reps):
                            # Pass adapter to simulator if using PostgreSQL
                            if use_postgres:
                                simulator = Simulator(db_adapter=db_adapter)
                            else:
                                simulator = Simulator()
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
                    exp_report_no_index = ExpReport(configs.experiment_id, constants.COMPONENT_NO_INDEX + exp_id_list[i], configs.reps,
                                                    configs.rounds)
                    for r in range(configs.reps):
                        results, total_workload_time = ConfigRunner.run(
                            "no_index.sql", uniform=UNIFORM)
                        temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                           constants.DF_COL_MEASURE_VALUE])
                        temp.append(
                            [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                        temp[constants.DF_COL_REP] = r
                        exp_report_no_index.add_data_list(temp)
                    exp_report_list.append(exp_report_no_index)

                # Running Optimal
                if OPTIMAL:
                    exp_report_optimal = ExpReport(
                        configs.experiment_id, constants.COMPONENT_OPTIMAL + exp_id_list[i], configs.reps, configs.rounds)
                    for r in range(configs.reps):
                        results, total_workload_time = ConfigRunner.run(
                            "optimal_config.sql", uniform=UNIFORM)
                        temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                           constants.DF_COL_MEASURE_VALUE])
                        temp.append(
                            [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                        temp[constants.DF_COL_REP] = r
                        exp_report_optimal.add_data_list(temp)
                    exp_report_list.append(exp_report_optimal)

                # Running DTA Optimal
                if TA_OPTIMAL:
                    exp_report_ta = ExpReport(
                        configs.experiment_id, constants.COMPONENT_TA_OPTIMAL + exp_id_list[i], configs.reps, configs.rounds)
                    for r in range(configs.reps):
                        dta_runner = DTARunner(
                            configs.ta_runs, workload_type=constants.TA_WORKLOAD_TYPE_OPTIMAL)
                        results, total_workload_time = dta_runner.run()
                        temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                           constants.DF_COL_MEASURE_VALUE])
                        temp.append(
                            [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                        temp[constants.DF_COL_REP] = r
                        exp_report_ta.add_data_list(temp)
                    exp_report_list.append(exp_report_ta)

                # Running DTA Full
                if TA_FULL:
                    exp_report_ta = ExpReport(configs.experiment_id, constants.COMPONENT_TA_FULL + exp_id_list[i], configs.reps,
                                              configs.rounds)
                    for r in range(configs.reps):
                        dta_runner = DTARunner(
                            [0], workload_type=constants.TA_WORKLOAD_TYPE_FULL)
                        results, total_workload_time = dta_runner.run()
                        temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                           constants.DF_COL_MEASURE_VALUE])
                        temp.append(
                            [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                        temp[constants.DF_COL_REP] = r
                        exp_report_ta.add_data_list(temp)
                    exp_report_list.append(exp_report_ta)

                # Running DTA Current
                if TA_CURRENT:
                    exp_report_ta = ExpReport(configs.experiment_id, constants.COMPONENT_TA_CURRENT + exp_id_list[i],
                                              configs.reps, configs.rounds)
                    for r in range(configs.reps):
                        dta_runner = DTARunner(
                            configs.ta_runs, workload_type=constants.TA_WORKLOAD_TYPE_CURRENT)
                        results, total_workload_time = dta_runner.run()
                        temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                           constants.DF_COL_MEASURE_VALUE])
                        temp.append(
                            [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                        temp[constants.DF_COL_REP] = r
                        exp_report_ta.add_data_list(temp)
                    exp_report_list.append(exp_report_ta)

                # Running DTA Schedule (everything from last run)
                if TA_SCHEDULE:
                    exp_report_ta = ExpReport(configs.experiment_id, constants.COMPONENT_TA_SCHEDULE + exp_id_list[i],
                                              configs.reps, configs.rounds)
                    for r in range(configs.reps):
                        dta_runner = DTARunner(
                            configs.ta_runs, workload_type=constants.TA_WORKLOAD_TYPE_SCHEDULE)
                        results, total_workload_time = dta_runner.run()
                        temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                           constants.DF_COL_MEASURE_VALUE])
                        temp.append(
                            [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                        temp[constants.DF_COL_REP] = r
                        exp_report_ta.add_data_list(temp)
                    exp_report_list.append(exp_report_ta)

                # Running DDQN
                if DDQN:
                    exp_report_mab = ExpReport(configs.experiment_id, constants.COMPONENT_MAB + exp_id_list[i],
                                               configs.reps, configs.rounds)
                    for r in range(configs.reps):
                        DDQNSimulator = get_bandit('ddqn')
                        simulator = DDQNSimulator()
                        results, total_workload_time = simulator.run()
                        temp = DataFrame(results, columns=[constants.DF_COL_BATCH, constants.DF_COL_MEASURE_NAME,
                                                           constants.DF_COL_MEASURE_VALUE])
                        temp.append(
                            [-1, constants.MEASURE_TOTAL_WORKLOAD_TIME, total_workload_time])
                        temp[constants.DF_COL_REP] = r
                        exp_report_mab.add_data_list(temp)
                    exp_report_list.append(exp_report_mab)

                # Save results
                with open(experiment_folder_path + "reports.pickle", "wb") as f:
                    pickle.dump(exp_report_list, f)

                if SEPARATE_EXPERIMENTS:
                    helper.plot_exp_report(configs.experiment_id,
                                           exp_report_list, PLOT_MEASURE, PLOT_LOG_Y)
                    helper.create_comparison_tables(
                        configs.experiment_id, exp_report_list)

        # plot line graphs
        if not SEPARATE_EXPERIMENTS:
            helper.plot_exp_report(configs.experiment_id,
                                   exp_report_list, PLOT_MEASURE, PLOT_LOG_Y)
            helper.create_comparison_tables(configs.experiment_id, exp_report_list)

    finally:
        # Disconnect PostgreSQL adapter if used
        if use_postgres and db_adapter:
            db_adapter.disconnect()
            print("Disconnected from PostgreSQL")


if __name__ == "__main__":
    main()
