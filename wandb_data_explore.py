import argparse
import pickle
import pandas as pd


def main(pickle_file, tau, d1, d2):
    with open(pickle_file, 'rb') as f:
        data = pickle.load(f)
    print(f"original data is ")
    print(data)
    print(f"a total of {len(data)} experiments")
    data_sorted = data.sort_values(by='rt_sum')
    data_sorted_transformed = data_sorted[['rt_sum', 'tau', 'delta1', 'delta2', 'lambda']]
    print(f" the perf is \n {data_sorted_transformed}")
    print(f"the best 10 is \n {data_sorted_transformed[0:10]}")

    df = data_sorted_transformed
    print(f"the perf from the request is\n {df[(df['tau']==8) & (df['delta1']==0.3) & (df['delta2']==0.002)]}")
    # __import__('ipdb').set_trace()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, default='job_shifting_1_MAB_80.pickle')
    parser.add_argument('--tau', type=int, default=8)
    parser.add_argument('--d1', type=float, default=0.3)
    parser.add_argument('--d2', type=float, default=0.002)
    args = parser.parse_args()

    main(args.file, args.tau, args.d1, args.d2)
