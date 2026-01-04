import csv
import pickle
import pandas as pd

if __name__ == "__main__":
    fn = 'project.csv'
    fn = 'project.pickle'
    f_ext = fn.split('.')[1]
    if f_ext == 'csv':
        with open(fn, 'r') as f:
            reader = csv.reader(f)
        header = next(reader)
        print('Header:', header)
        for row in reader:
            print("Row:", row)
    elif f_ext == 'pickle':
        with open(fn, 'rb') as f:
            pd_data: pd.DataFrame = pickle.load(f)
        print(pd_data[:10])
    else:
        raise ValueError(f'ext {f_ext} not supported yet')