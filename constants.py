import logging
import os
from shared.minimal_len_colnames import get_db_colname_prefixs

# ===============================  Program Related  ===============================
DB_CONFIG = '/config/db.conf'
EXPERIMENT_FOLDER = '/experiments'
WORKLOADS_FOLDER = '/resources/workloads'
# EXPERIMENT_CONFIG = '\config\exp.conf'
EXPERIMENT_CONFIG = '/config/exp.conf'
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGGING_LEVEL = logging.INFO

TABLE_SCAN_TIME_LENGTH = 1000

# ===============================  Database / Workload  ===============================
SCHEMA_NAME = 'dbo'
PG_PAGE_SIZE = 8192
PG_SEQ_PAGE_COST = 1
PG_CPU_TUPLE_COST = 0.01
PG_CPU_OPERATOR_COST = 0.0025
PG_CPU_INDEX_TUPLE_COST = 0.005
PG_RANDOM_PAGE_COST = 4
HYPO_INDEX = True

# ===============================  Arm Generation Heuristics  ===============================
INDEX_INCLUDES = 1
MAX_PERMUTATION_LENGTH = 2
SMALL_TABLE_IGNORE = 10000
TABLE_MIN_SELECTIVITY = 0.2
PREDICATE_MIN_SELECTIVITY = 0.01

# ===============================  Bandit Parameters  ===============================
ALPHA_REDUCTION_RATE = 1.05
QUERY_MEMORY = 1
BANDIT_FORGET_FACTOR = 0.6
MAX_INDEXES_PER_TABLE = 6
CREATION_COST_REDUCTION_FACTOR = 3
STOP_EXPLORATION_ROUND = 500
UNIFORM_ASSUMPTION_START = 10

# ===============================  Reward Related  ===============================
COST_TYPE_ELAPSED_TIME = 1
COST_TYPE_CPU_TIME = 2
COST_TYPE_SUB_TREE_COST = 3
# COST_TYPE_CURRENT_EXECUTION = COST_TYPE_ELAPSED_TIME
# COST_TYPE_CURRENT_CREATION = COST_TYPE_ELAPSED_TIME
COST_TYPE_CURRENT_EXECUTION = COST_TYPE_SUB_TREE_COST
COST_TYPE_CURRENT_CREATION = COST_TYPE_SUB_TREE_COST

# ===============================  Context Related  ===============================
CONTEXT_UNIQUENESS = 0
CONTEXT_INCLUDES = False
STATIC_CONTEXT_SIZE = 3

# ===============================  Reporting Related  ===============================
DF_COL_COMP_ID = "Component"
DF_COL_REP = "Rep"
DF_COL_BATCH = "Batch Number"
DF_COL_BATCH_COUNT = "# of Batches"
DF_COL_MEASURE_NAME = "Measurement Name"
DF_COL_MEASURE_VALUE = "Measurement Value"

MEASURE_TOTAL_WORKLOAD_TIME = "Total Workload Time"
MEASURE_INDEX_CREATION_COST = "Index Creation Time"
MEASURE_INDEX_RECOMMENDATION_COST = "Index Recommendation Cost"
MEASURE_QUERY_EXECUTION_COST = "Query Execution Cost"
MEASURE_MEMORY_COST = "Memory Cost"
MEASURE_BATCH_TIME = "Batch Time"
MEASURE_HYP_BATCH_TIME = "Hyp Batch Time"

COMPONENT_MAB = "MAB"
COMPONENT_TA_OPTIMAL = "TA_OPTIMAL"
COMPONENT_TA_FULL = "TA_FULL"
COMPONENT_TA_CURRENT = "TA_CURRENT"
COMPONENT_TA_SCHEDULE = "TA_SCHEDULE"
COMPONENT_OPTIMAL = "OPTIMAL"
COMPONENT_NO_INDEX = "NO_INDEX"
COMPONENT_DDQN = "DDQN"
COMPONENT_DDQN_SINGLE_COLUMN = "DDQN_SINGLE_COLUMN"

TA_WORKLOAD_TYPE_OPTIMAL = 'optimal'
TA_WORKLOAD_TYPE_FULL = 'full'
TA_WORKLOAD_TYPE_CURRENT = 'current'
TA_WORKLOAD_TYPE_SCHEDULE = 'schedule'

# ===============================  Other  ===============================
TABLE_SCAN_TIMES = {"SSB": {"customer": [], "dwdate": [], "lineorder": [], "part": [], "supplier": []},
                    "TPCH": {"LINEITEM": [], "CUSTOMER": [], "NATION": [], "ORDERS": [], "PART": [], "PARTSUPP": [],
                             "REGION": [], "SUPPLIER": []},
                    "TPCHSKEW": {"LINEITEM": [], "CUSTOMER": [], "NATION": [], "ORDERS": [], "PART": [], "PARTSUPP": [],
                                 "REGION": [], "SUPPLIER": []},
                    "TPCDS": {"CALL_CENTER": [], "CATALOG_PAGE": [], "CATALOG_RETURNS": [], "CATALOG_SALES": [],
                              "CUSTOMER": [], "CUSTOMER_ADDRESS": [], "CUSTOMER_DEMOGRAPHICS": [], "DATE_DIM": [],
                              "DBGEN_VERSION": [], "HOUSEHOLD_DEMOGRAPHICS": [], "INCOME_BAND": [], "INVENTORY": [],
                              "ITEM": [], "PROMOTION": [], "REASON": [], "SHIP_MODE": [], "STORE": [],
                              "STORE_RETURNS": [], "STORE_SALES": [], "TIME_DIM": [], "WAREHOUSE": [], "WEB_PAGE": [],
                              "WEB_RETURNS": [], "WEB_SALES": [], "WEB_SITE": []},
                    "IMDB": {"AKA_NAME": [], "AKA_TITLE": [], "CAST_INFO": [], "CHAR_NAME": [],
                             "COMP_CAST_TYPE": [], "COMPANY_NAME": [], "COMPANY_TYPE": [], "COMPLETE_CAST": [],
                              "INFO_TYPE": [], "KEYWORD": [], "KIND_TYPE": [], "LINK_TYPE": [],
                              "MOVIE_COMPANIES": [], "MOVIE_INFO": [], "MOVIE_INFO_IDX": [], "MOVIE_KEYWORD": [], "MOVIE_LINK": [],
                              "NAME": [], "PERSON_INFO": [], "ROLE_TYPE": [], "TITLE": []}
                    }

tpch_db_colname_prefixs = get_db_colname_prefixs('tpch010', save=True)
tpcds_db_colname_prefixs = get_db_colname_prefixs('indexselection_tpcds___10', save=True)
imdb_job_colname_prefixs = get_db_colname_prefixs('indexselection_job___1', save=True)
# db_colname_prefixs = tpch_db_colname_prefixs.copy()
# db_colname_prefixs.update(tpcds_db_colname_prefixs)
# db_colname_prefixs = imdb_job_colname_prefixs

