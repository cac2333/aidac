import random
import time

import pandas as pd
import csv
import regex as re

import psycopg

from benchmarks.dbc import *

config = __import__('table_config')

_PATH = getattr(config, 'local_data_path')

date_columns = getattr(config, 'date_columns')
db_config = getattr(config, 'db_config')
random.seed(37)

SF = 1

table_size = {
    'customer': 150000 * 8,
    'lineitem': 6001215 * 16,
    'nation': 25*4,
    'orders': 1500000 * 9,
    'part': 200000 * 9,
    'partsupp': 800000* 5,
    'region': 5*3,
    'supplier': 1000*7
}


def read_file(table, parse_dates=False):
    print(f'reading {table}')
    return pd.read_csv(_PATH+table+'.csv', parse_dates=parse_dates)


def read_tables(local_tbs, remote_tbs):
    all_tbs = {}
    for p in local_tbs:
        if p in date_columns:
            all_tbs[p] = read_file(p, parse_dates=date_columns[p])
        else:
            all_tbs[p] = read_file(p)
    return all_tbs


def read_auto_gen_queries(path):
    with open(path, 'r') as f:
        return f.readlines()


def extract_query_meta(query):
    patterns = {
        'projection': r'\[\[[^\[]*\]\]',
        'filter': r'(<|>|\||==|&)',
        'join': r'merge',
        'groupby': 'groupby',
        'aggregation': 'agg'
    }

    stat = []
    for op, p in patterns.items():
        m = re.findall(p, query)
        stat.append(len(m))

    return stat


def output_size(df):
    shape = df.shape
    if len(shape) > 1:
        return [shape[0], shape[1]]
    else:
        return [shape[0], 0]


def collect_stats(job, table_list, opath, id):
    rpath = f'{opath}/merged_queries_auto_sf0001.txt'
    logpath = f'{opath}/out_{id}_meta.txt'
    query_stats_log = f'{opath}/query_input_size_{id}.csv'
    locals, remotes = read_specs(logpath)

    # start index, end index and step for iterating the queries
    sc, ec, step = 0, 3000, 10
    # total query count and number of errors
    idx, err_count = 0, 0

    queries = read_auto_gen_queries(rpath)[sc:ec:step]
    indexes = range(sc, ec, step)
    all_query_stats = []

    for cmd in queries:
        # find the remote tables
        cmd = cmd.rstrip()
        local_size = 0
        remote_size = 0
        for tb in remotes:
            if tb in cmd:
                remote_size += table_size[tb] * SF
        for tb in locals:
            if tb in cmd:
                local_size += table_size[tb] * SF
        total_size = local_size + remote_size

        stats = [indexes[idx], local_size, remote_size, total_size]

        all_query_stats.append(stats)
        # writer.writerow([indexes[idx], times[idx]])
        idx += 1

    with open(query_stats_log, 'w') as qf:
        writer = csv.writer(qf)
        writer.writerows(all_query_stats)


if __name__ == "__main__":
    table_list = ['nation', 'customer', 'orders', 'lineitem', 'part', 'partsupp', 'region', 'supplier']
    dbc = DBConfig(db_config)

    query_path = 'auto_gen_queries'
    for id in range(0, 5):
        random.seed(id*10)
        collect_stats('p1', table_list, query_path, id)



