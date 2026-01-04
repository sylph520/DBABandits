import pandas as pd
import wandb
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--exp_id', type=str, default='')
args = parser.parse_args()

api = wandb.Api()
entity, project = "sylph", "dlinucb-ablation"
runs = api.runs(entity + "/" + project, order="+created_at")

pd_rows = []
for run in runs:
    # .summary contains output keys/values for
    # metrics such as accuracy.
    #  We call ._json_dict to omit large files
    smy = run.summary._json_dict
    if 'round_time_sum' not in smy:
        continue
    pd_row = {}
    pd_row['name'] = run.name

    conf_dict = run.config
    conf_dict.pop('db_type')
    conf_dict.pop('variedW_id')
    conf_dict.pop('dynamic_flag')
    conf_dict.pop('shuffle_flag')
    pd_row.update(conf_dict)

    pd_row['rt_sum'] = smy['round_time_sum']
    pd_row['rt_list'] = smy['round_time_list']

    pd_rows.append(pd_row)

    # .config contains the hyperparameters.
    #  We remove special values that start with _.
    # config_list.append({k: v for k, v in run.config.items() if not k.startswith("_")})

    # .name is the human-readable name of the run.
# runs_df = pd.DataFrame(
#     {"summary": summary_list, "config": config_list, "name": name_list}
# )
runs_df = pd.DataFrame(pd_rows)

# exp_id options
# {
#  'tpcds_shifting_1_MAB_80',
#  'tpch_static_1_MAB',
#  'job_shifting_1_MAB_80',
#  'tpch_shifting_1_MAB_80',
#  'tpcds_random_10_MAB_rep20_round20',
#  'job_random_1_MAB_rep16_round20',
#  'job_static_1_MAB',
#  'tpch_random_1_MAB_rep20_round20',
#  'tpcds_static_10_MAB'
#  }

exp_id_to_save = args.exp_id
__import__('ipdb').set_trace()
if not exp_id_to_save:
    save_fn = 'project'
    save_fn = 'project3'
    save_fn = 'project5'
    if os.path.exists(save_fn + '.csv') or os.path.exists(save_fn + '.pickle'):
        raise ValueError(f"{save_fn} already existed, please check")
    else:
        runs_df.to_csv(f"{save_fn}.csv", index=False)
        runs_df.to_pickle(f"{save_fn}.pickle")
else:
    runs_df_group = runs_df[runs_df['exp_id'] == exp_id_to_save]
    save_fn = exp_id_to_save
    if os.path.exists(save_fn + '.csv') or os.path.exists(save_fn + '.pickle'):
        raise ValueError(f"{save_fn} already existed, please check")
    else:
        runs_df_group.to_csv(f"{save_fn}.csv", index=False)
        runs_df_group.to_pickle(f"{save_fn}.pickle")
