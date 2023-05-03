import csv

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

path = 'tpch_out'
img_path = f'{path}/img'
sf = '01'
pd_path = f'{path}/sf{sf}_pd.csv'
lad_path = f'{path}/sf{sf}_lad.csv'
opt_path = f'{path}/sf{sf}_opt.csv'
header = ['qid', 'local_tb', 'remote_tb', 'runtime']

lookup = {
    'lineitem': 'l',
    'orders': 'o',
    'part': 'p',
    'partsupp': 'ps',
    'supplier': 's',
    'customer': 'c',
    'nation': 'n',
    'region': 'r'
}

def plot_1_result(qry_name, records):
    n = len(records)
    fig, axes = plt.subplots(1, n, figsize=(5*n, 7), sharey=True, constrained_layout=True)

    axes = [axes] if n <= 1 else axes
    left_most = True

    # rec: index, qid, local_tb, remote_tb, lad_time, pd_time, opt_time
    # extract the table distribution into locals and remotes, extract the result runtime into ys
    for ax, rec in zip(axes, records.iterrows()):
        rec = rec[1]
        xs = ['LAD', 'Pandas', 'Heuristic']
        ys = rec[4:7]

        locals = ' '.join([lookup[x] for x in rec[2].split(' ')])
        remotes = ' '.join([lookup[x] for x in rec[3].split(' ')])

        sns.barplot(x=xs, y=ys, ax=ax, alpha=.6)
        ax.set(ylabel='Runtime')
        ax.bar_label(ax.containers[0], fmt='%.3f')

        ax.set_title(f'local: {locals}\nremote: {remotes}')

    fig.tight_layout()
    fig.legend()

    title = qry_name.split('_')
    title = ''.join(title).upper()[:-2]
    fig.suptitle('Sample query: '+title)

    plt.subplots_adjust(wspace=.15, top=.85)
    fig.tight_layout()

    # fig.show()
    # plt.savefig(f'{img_path}/{title.lower()}.png', format='png')

    fig.savefig(f'{img_path}/{title.lower()}.pdf', format='pdf', bbox_inches='tight', pad_inches=0)


def plot_sum(df):
    rule_sum = df['hm_time'].sum()
    aidac_sum = df['lad_time'].sum()
    pd_sum = df['pd_time'].sum()
    fig = plt.figure(facecolor=(1, 1, 1))

    # normalize by dividing the sum of rule-based method times
    ys = [aidac_sum/rule_sum, pd_sum/rule_sum, 1]
    xs = ['LAD', 'Pandas', 'Heuristic']

    ax = sns.barplot(x=xs, y=ys, alpha=.6)
    ax.bar_label(ax.containers[0], fmt='%.3f')
    plt.title('Total time normalized by the runtime of'
              '\nmanually-optimized method')

    # plt.show()
    # fig.savefig(f'{img_path}/sum.png', format='png')

    fig.savefig(f'{img_path}/sum.pdf', format='pdf', bbox_inches='tight', pad_inches=0)

def cleanup_data():
    lad_df = pd.read_csv(lad_path, names=['qid', 'local_tb', 'remote_tb', 'lad_time'], header=0)
    pd_df = pd.read_csv(pd_path, names=['qid', 'local_tb', 'remote_tb', 'pd_time'], header=0)
    opt_df = pd.read_csv(opt_path, names=['qid', 'local_tb', 'remote_tb', 'hm_time'], header=0)

    #put all data in a single df
    combined = lad_df.merge(pd_df, on=['qid', 'local_tb', 'remote_tb'], how='inner').merge(opt_df, on=['qid', 'local_tb', 'remote_tb'], how='inner')
    combined.reset_index(inplace=True)
    return combined


if __name__ == '__main__':
    combined = cleanup_data()

    plt.rcParams['pdf.fonttype'] = 42
    plt.rcParams['ps.fonttype'] = 42
    plt.rcParams.update({'font.size': 15})

    sns.set(rc={'figure.figsize': (10, 7)})
    sns.set(font_scale=1.5)

    qids = combined['qid'].unique()
    for qid in qids:
        plot_1_result(qid, combined[combined['qid'] == qid])

    # plot_sum(combined)
    # f = plt.figure(figsize=(4, 5))
