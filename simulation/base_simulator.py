from typing import Dict

from database.dbconn import DBConnection
from database.query_v5 import Query
from bandits.bandit_arm import BanditArm
from bandits.oracle_v2 import OracleV7 as Oracle
import shared.helper as helper
import constants


# Simulation built on vQ to collect the super arm performance
class BaseSimulator:
    def __init__(self, kwargs: dict = {
        "db_conf": {
            "db_type": "MSSQL",
            "database": "tpch_010"
        },
        "exp_conf": {
            "exp_id": "tpc_h_static_10_MAB"
        }}
    ):
        """
        setup queries (self.queries), db connection (self.connection),
        and an empty query_object_store
        """
        # # configuring the logger
        # logging.basicConfig(
        #     filename=helper.get_experiment_folder_path(configs.experiment_id) + configs.experiment_id + '.log',
        #     filemode='w', format='%(asctime)s - %(levelname)s - %(message)s')
        # logging.getLogger().setLevel(logging.INFO)

        db_conf_dict = kwargs['db_conf']
        self.exp_config = kwargs['exp_conf']

        self.db_type = db_conf_dict['db_type']
        self.connection = DBConnection(db_conf_dict)

        # Reading the configuration for given experiment ID, and get the query List
        # experiment id for the current run
        workload_file = str(self.exp_config.workload_file)
        self.queries = helper.get_queries_v2(workload_file)  # fetch query instances from files
        self.all_columns, self.number_of_columns = self.connection.get_all_columns()

        self.query_obj_store: Dict[int, Query] = {}
        self.bandit_arms_store: Dict[str, BanditArm] = {}

        self.context_size = self.number_of_columns * (
            1 + constants.CONTEXT_UNIQUENESS + constants.CONTEXT_INCLUDES) + constants.STATIC_CONTEXT_SIZE
        self.max_memory = self.exp_config.max_memory - int(self.connection.get_current_pds_size())
        self.oracle = Oracle(self.max_memory)

