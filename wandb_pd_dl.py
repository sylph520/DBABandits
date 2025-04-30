import pandas as pd
import wandb
import os

api = wandb.Api()
entity, project = "sylph", "dlinucb-ablation"
runs = api.runs(entity + "/" + project)

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
runs_df = pd.DataFrame(
   pd_rows
)

save_fn = 'project'
save_fn = 'project3'
if os.path.exists(save_fn + '.csv') or os.path.exists(save_fn + '.pickle'):
    raise ValueError(f"{save_fn} already existed, please check")
else:
    runs_df.to_csv(f"{save_fn}.csv", index=False)
    runs_df.to_pickle(f"{save_fn}.pickle")
