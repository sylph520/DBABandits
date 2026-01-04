from exp_data_parse import parse_float_seqs
import argparse
from matplotlib import pyplot as plt
import ast
import os


def plot_bars(x, y, type='tpch', figname='rttest'):
    print(x)
    print(y)

    ymin = min(y) - 0.001 * 10**8  # Adjust the buffer as needed
    ymax = max(y) + 0.001 * 10**8  # Adjust the buffer as needed
    plt.ylim(ymin, ymax)

    bars = plt.bar([str(i) for i in x], y)
    plt.tight_layout()
    for bar in bars:
        yval = bar.get_height()
        if type == 'tpch':
            plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval / 1e7, 3), ha='center', va='bottom', fontsize=10)
        elif type == 'tpcds':
            plt.text(bar.get_x() + bar.get_width() / 2, yval, round(yval / 1e9, 3), ha='center', va='bottom', fontsize=10)
        else:
            raise
    plt.savefig(figname + '.jpg')
    plt.savefig(figname + '.pdf')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', type=str, default='logs/rt_ablation_delta2_20250414_144203.txt')
    parser.add_argument('--param_list', type=str, default='')
    args = parser.parse_args()

    fn = 'logs/rt_ablation_delta2_20250414_144203.txt'
    fn = args.file
    basefn = os.path.basename(fn).split('.')[0]
    figname = basefn + '_bar'
    print(figname)

    param_list = args.param_list

    import pdb; pdb.set_trace()
    plist = ast.literal_eval(param_list)
    rt_list = parse_float_seqs(fn)
    # print(rt_list)
    plot_bars(plist, [sum(i) for i in rt_list], figname=figname)
