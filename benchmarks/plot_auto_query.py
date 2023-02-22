import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


index_range = [3]
path = 'auto_gen_queries/'
lad_prefix = path + 'out_{}.txt'
pd_prefix = path + 'out_{}_pd.txt'
input_size_prefix = path + 'query_input_size_{}.csv'

fig_dim = (15, 12)


def plot_hist(df, title, range_min):
    # plotting two histograms on the same axis
    # plt.hist(df['lad_time'], bins=50, alpha=0.45, color='red', log=True)
    # plt.hist(df['pd_time'], bins=50, alpha=0.45, color='blue', log=True)
    max_range = max(df['lad_time'].max(), df['pd_time'].max())
    binrange = (0, max_range)

    fig, ax = plt.subplots()

    ax = sns.histplot(data=df['lad_time'],  label='runtime',  color='#f0958f', element='step', ax=ax,
                 stat='count', binwidth=.4, binrange=binrange, alpha=.4)
    sns.histplot(data=df['pd_time'],  label='runtime',  color='skyblue', element='step', ax=ax,
                 stat='count',  binwidth=.4, binrange=binrange, alpha=.4)
    ax.set(xlabel='runtime', ylabel='count')

    ax2 = plt.axes([.45, .4, .38, .38], facecolor='seashell')
    sns.histplot(data=df['lad_time'], label='runtime', color='#f0958f', element='step', ax=ax2,
                 stat='count', binwidth=.4, binrange=binrange, alpha=.4)
    sns.histplot(data=df['pd_time'], label='runtime', color='skyblue', element='step', ax=ax2,
                 stat='count', binwidth=.4, binrange=binrange, alpha=.4)
    ax2.set_title('zoom')
    ax2.set(xlabel='runtime', ylabel='count')
    ax2.set_xlim([6, max_range + 1])
    ax2.set_ylim([0, 5])

    ax.set_title(title)

    plt.legend(['LAD',
                'Pandas'])

    plt.show()


def plot_scatter(df, title):

    def round_result(x, y):
        return round(x / y, 2)

    df['size_ratio'] = df.apply(lambda x: round_result(x['local_size'], x['total_size']), axis=1)
    data = df[['pd_time', 'lad_time', 'size_ratio']]
    dfm = data.melt('size_ratio', var_name='method', value_name='runtime')

    # fig, ax = plt.subplots(figsize=fig_dim)
    # sns.set(rc={'figure.figsize': (12, 12)})
    # sns.set(rc={'figure.figsize': (12, 12), 'figure.dpi':300})

    ax = sns.catplot(x="size_ratio",
                     y="runtime", data=dfm, hue='method', legend=True,
                     palette=['skyblue', '#f0958f'],alpha=.4)
    ax.set(ylabel='runtime', xlabel='local input size / total input size')
    # ax.set_xticklabels(ax.get_xticklabels(), rotation=30)
    plt.xticks(rotation=45)
    plt.title(title)
    plt.show()



if __name__ == '__main__':
    sns.set(rc={'figure.figsize': (10, 7)})
    # read and group data
    lad_dfs = []
    pd_dfs = []
    input_dfs = []
    combined_dfs = []
    for idx in index_range:
        lad_df = pd.read_csv(lad_prefix.format(idx), names=['id', 'lad_time'], header=None)
        lad_dfs.append(lad_df)

        pd_df = pd.read_csv(pd_prefix.format(idx), names=['id', 'pd_time'], header=None)
        pd_dfs.append(pd_df)

        input_df = pd.read_csv(input_size_prefix.format(idx), names=['id', 'local_size', 'remote_size', 'total_size'],
                               header=None)
        input_dfs.append(input_df)

        combined_dfs.append(lad_df.merge(pd_df, on='id', how='inner').merge(input_df, on='id', how='inner'))

    # clean data
    cleaned_dfs = [df[df['lad_time'] > 0] for df in combined_dfs]
    all_cleaned = pd.concat(cleaned_dfs)

    # plot histogram
    plot_hist(all_cleaned, 'All data combined', 0)

    for id, df in zip(index_range, cleaned_dfs):
        plot_hist(df, f'Distribution {id} (for all)', 5)
        # plot_hist(df, f'Distribution {id} (>5)', 5)
        plot_scatter(df, f'Distribution {id}')
        plot_scatter(df, f'Runtime vs local/total table size for {id}')



    # todo: have a summary of the LAD plans, percentage/stats of the queries
    # show the figures
    # change a template, mcgill thesis
    # increase SF for one approach ()
    # purpose of using the randomly generated queries (large scale experiments),
    # effects of increasing datasets,
    # merge the experiments and discussion together, tell the story