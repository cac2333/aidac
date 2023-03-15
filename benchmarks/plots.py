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

path = 'tpch_out'
img_path = f'{path}/img'
sf = '01'
pd_path = f'{path}/sf{sf}_pd.csv'
lad_path = f'{path}/sf{sf}_lad.csv'
opt_path = f'{path}/sf{sf}_opt.csv'
header = ['qid', 'local_tb', 'remote_tb', 'runtime']

def plot_1_result(qry_name, records):
    n = len(records)
    fig, axes = plt.subplots(1, n, figsize=(5*n, 7), sharey=True, constrained_layout=True)

    axes = [axes] if n <= 1 else axes
    left_most = True

    # rec: index, qid, local_tb, remote_tb, lad_time, pd_time, opt_time
    for ax, rec in zip(axes, records.iterrows()):
        rec = rec[1]
        xs = ['LAD', 'Pandas', 'Heuristic']
        ys = rec[4:7]

        locals = rec[2]
        remotes = rec[3]

        sns.barplot(x=xs, y=ys, ax=ax, alpha=.6)
        # sns.histplot(data=df['pd_time'], label='runtime', color='skyblue', element='step', ax=ax,
        #              stat='count', binwidth=.4, binrange=binrange, alpha=.4)
        ax.set(xlabel='Method', ylabel='Runtime')

        # ax2 = plt.axes([.45, .4, .38, .38], facecolor='seashell')
        # sns.histplot(data=df['lad_time'], label='runtime', color='#f0958f', element='step', ax=ax2,
        #              stat='count', binwidth=.4, binrange=binrange, alpha=.4)
        # sns.histplot(data=df['pd_time'], label='runtime', color='skyblue', element='step', ax=ax2,
        #              stat='count', binwidth=.4, binrange=binrange, alpha=.4)
        # ax2.set_title('zoom')
        # ax2.set(xlabel='runtime', ylabel='count')
        # ax2.set_xlim([6, max_range + 1])
        # ax2.set_ylim([0, 5])

        ax.set_title(f'local: {locals}\nremote: {remotes}')

        left_most = False
    fig.tight_layout()
    fig.legend()

    title = qry_name.split('_')
    title = ''.join(title).upper()[:-2]
    fig.suptitle('Sample query: '+title)

    plt.subplots_adjust(wspace=.15, top=.85)
    fig.tight_layout()
    # fig.show()
    plt.savefig(f'{img_path}/{title.lower()}.png', format='png')


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

def plot_sum(df):
    rule_sum = df['hm_time'].sum()
    aidac_sum = df['lad_time'].sum()
    pd_sum = df['pd_time'].sum()
    fig = plt.figure(facecolor=(1, 1, 1))
    ys = [aidac_sum/rule_sum, pd_sum/rule_sum, 1]
    xs = ['LAD', 'Pandas', 'Heuristic']

    ax = sns.barplot(x=xs, y=ys, alpha=.6)
    ax.bar_label(ax.containers[0], fmt='%.3f')
    plt.title('Total time normalized by the runtime of'
              '\nmanually-optimized method')

    print(ys)
    # plt.show()

    fig.savefig(f'{img_path}/sum.png', format='png')
    fig.savefig(f'{img_path}/sum.eps', format='eps' ,bbox_inches='tight', pad_inches=0)

def cleanup_data():
    lad_df = pd.read_csv(lad_path, names=['qid', 'local_tb', 'remote_tb', 'lad_time'], header=0)
    pd_df = pd.read_csv(pd_path, names=['qid', 'local_tb', 'remote_tb', 'pd_time'], header=0)

    opt_df = pd.read_csv(opt_path, names=['qid', 'local_tb', 'remote_tb', 'hm_time'], header=0)
    combined = lad_df.merge(pd_df, on=['qid', 'local_tb', 'remote_tb'], how='inner').merge(opt_df, on=['qid', 'local_tb', 'remote_tb'], how='inner')
    combined.reset_index(inplace=True)
    return combined

if __name__ == '__main__':
    combined = cleanup_data()

    colors = ['#f0958f', 'skyblue', 'indigo']
    sns.set_palette(colors)
    sns.set(rc={'figure.figsize': (10, 7)})
    plt.rcParams['pdf.fonttype'] = 42
    plt.rcParams['ps.fonttype'] = 42
    plt.rcParams.update({'font.size': 15})

    # qids = combined['qid'].unique()
    # for qid in qids:
    #     plot_1_result(qid, combined[combined['qid'] == qid])

    plot_sum(combined)
    f = plt.figure(figsize=(4, 5))



    # plot_sum()
    # for qry, records in data.items():
    #     plot_1_result(qry, records)