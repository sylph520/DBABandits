import pickle
import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt
import  argparse


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--exp_id', type=str, default='tpch_static_1_MAB')
    args = parser.parse_args()

    with open('project.pickle', 'rb') as f:
        df = pickle.load(f)
        df['rt_sum'] = df['rt_sum']/(1e7)
        # assert isinstance(df['rt_sum'], float64)

    tpch_static_1_MAB_df = df[df['exp_id']=='tpch_static_1_MAB']
    df = tpch_static_1_MAB_df[tpch_static_1_MAB_df['tau']<100]
    # fig = px.scatter_3d(df, x='lambda', y='delta1', z='delta2', color='rt_sum')
    # fig = px.scatter_matrix(df, 
    #                         # dimensions=['lambda', 'delta1', 'tau', 'delta2'], 
    #                         dimensions=['delta1', 'tau', 'delta2'], 
    #                         # dimensions=['tau', 'delta2'],
    #                         color='rt_sum',
    #                         # color_continuous_scale=px.colors.sequential.Viridis
    #                         )
    pd.plotting.scatter_matrix(df[['lambda', 'delta1', 'tau', 'delta2']],
                               c=df['rt_sum']
                               )
    # plt.show()