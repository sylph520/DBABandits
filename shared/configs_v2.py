import configparser
import json

import constants as constants

# Reading the configuration for given experiment ID
exp_config = configparser.ConfigParser()
exp_config.read(constants.ROOT_DIR + constants.EXPERIMENT_CONFIG)

# experiment id for the current run
experiment_id = exp_config['general']['run_experiment']

# information about experiment
reps = int(exp_config[experiment_id]['reps'])
rounds = int(exp_config[experiment_id]['rounds']) # 25
hyp_rounds = int(exp_config[experiment_id]['hyp_rounds']) #  0
# e.g., [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
workload_shifts = json.loads(exp_config[experiment_id]['workload_shifts'])
# e.g., [0, 21, 42, 63, 84, 105, 126, 147, 168, 189, 210, 231, 252, 273, 294, 315, 336, 357, 378, 399, 420, 441, 462, 483, 504]
queries_start_list = json.loads(exp_config[experiment_id]['queries_start'])
# e.g., [21, 42, 63, 84, 105, 126, 147, 168, 189, 210, 231, 252, 273, 294, 315, 336, 357, 378, 399, 420, 441, 462, 483, 504, 525]
queries_end_list = json.loads(exp_config[experiment_id]['queries_end'])

config_shifts = json.loads(exp_config[experiment_id]['config_shifts'])
config_start_list = json.loads(exp_config[experiment_id]['config_start'])
config_end_list = json.loads(exp_config[experiment_id]['config_end'])
ta_runs = json.loads(exp_config[experiment_id]['ta_runs'])
ta_workload = str(exp_config[experiment_id]['ta_workload'])
workload_file = str(exp_config[experiment_id]['workload_file'])
components = json.loads(exp_config[experiment_id]['components'])
mab_versions = json.loads(exp_config[experiment_id]['mab_versions'])

# constraints
max_memory = int(exp_config[experiment_id]['max_memory'])

# hyper parameters
input_alpha = float(exp_config[experiment_id]['input_alpha'])
input_lambda = float(exp_config[experiment_id]['input_lambda'])
