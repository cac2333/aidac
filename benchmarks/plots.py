import csv

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

hm = {
    'id': 0,
    'locals': 1,
    'remotes': 2,
    'pd_card': 3,
    'pd_time': 4,
    'pd_mem': 5,
    'aidac_card': 6,
    'aidac_time': 7,
    'aidac_mem': 8,
    'opt_card': 9,
    'rule_based_time':10,
    'opt_mem': 11
}

path = 'C:\\school\\M2\\aidac_report\\result_out_sf01_v2.csv'

def plot_1_result(qry_name, records):
    n = len(records)
    names = ['pd', 'aidac', 'rule_based']
    colors = ['orange', 'darkseagreen', 'steelblue']
    fig, axes = plt.subplots(1, n, figsize=(5*n, 7), sharey=True, constrained_layout=True)

    axes = [axes] if n <= 1 else axes
    left_most = True

    for ax, rec in zip(axes, records):
        values = [float(rec[name+'_time']) if rec[name+'_time'] else 0 for name in names]
        ax.bar(names, values, color=colors, width=.9)
        # sns.barplot(data=values, ax=ax)

        if left_most:
            ax.set_ylabel('Seconds')

        locals = rec['locals']
        remotes = rec['remotes']
        title = f'local: {locals}\nremote: {remotes}'
        ax.set_title(title)

        left_most = False
    fig.tight_layout()
    fig.legend()

    title = qry_name.split('_')
    title = ''.join(title).upper()
    fig.suptitle('Sample query: '+title)

    plt.subplots_adjust(wspace=.15, top=.85)

    fig.show()


def extract_data(ddict, row):
    qry = row[hm['id']]
    record = {}
    headers = ['locals', 'remotes', 'pd_card', 'pd_time', 'aidac_card', 'aidac_time', 'opt_card', 'rule_based_time']
    for h in headers:
        record[h] = row[hm[h]]

    if qry in ddict:
        ddict[qry].append(record)
    else:
        ddict[qry] = [record]

def plot_sum():
    df = pd.read_csv(path)
    rule_sum = df['opt_time'].sum()
    aidac_sum = df['aidac_time'].sum()
    pd_sum = df['pd_time'].sum()

    pdata = [pd_sum/rule_sum, aidac_sum/rule_sum, 1]

    names = ['pd', 'aidac', 'manual-opt']
    colors = ['orange', 'darkseagreen', 'steelblue']
    # ax = f.add_axes()
    plt.bar(names, pdata, color=colors, width=.9)
    plt.title('Total time comparing to '
              '\nmanually-optimized')
    plt.show()

if __name__ == '__main__':
    data = {}
    with open(path, 'r') as f:
        cf = csv.reader(f)
        for id, row in enumerate(cf):
            if id==0:
                continue
            extract_data(data, row)

    print(data)

    print(plt.rcParams.keys())
    f = plt.figure(figsize=(4, 5))

    plt.rcParams['font.size'] = 12
    plt.rcParams['figure.titlesize'] = 16
    plt.rcParams['figure.subplot.wspace'] =.5
    plt.grid(visible=True, axis='y', linewidth=1)
    # plt.rcParams['font.weight'] = 'bold'

    plot_sum()
    # for qry, records in data.items():
    #     plot_1_result(qry, records)