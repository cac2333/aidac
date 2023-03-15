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


def run_one_dist(job, table_list, opath, id, con):
    rpath = f'{opath}/merged_queries_auto_sf0001.txt'
    wpath = f'{opath}/out_{id}_pd.txt'
    logpath = f'{opath}/out_{id}_meta.txt'
    query_stats_log = f'{opath}/query_stats.csv'
    locals, remotes = read_specs(logpath)

    dist = Dist(job, table_list, locals, remotes, True)

    nation = dist.nation if hasattr(dist, 'nation') else None
    customer = dist.customer if hasattr(dist, 'customer') else None
    orders = dist.orders if hasattr(dist, 'orders') else None
    lineitem = dist.lineitem if hasattr(dist, 'lineitem') else None
    part = dist.part if hasattr(dist, 'part') else None
    partsupp = dist.partsupp if hasattr(dist, 'partsupp') else None
    region = dist.region if hasattr(dist, 'region') else None
    supplier = dist.supplier if hasattr(dist, 'supplier') else None

    # start index, end index and step for iterating the queries
    sc, ec, step = 0, 3000, 3
    # total query count and number of errors
    idx, err_count = 0, 0

    queries = read_auto_gen_queries(rpath)[sc:ec:step]
    indexes = range(sc, ec, step)
    times = [0] * len(queries)
    all_query_stats = []

    # write to result file
    # with open(query_stats_log, 'a') as f:
    with open(wpath, 'a') as f:
        writer = csv.writer(f)

        for cmd in queries:
            # find the remote tables
            cmd = cmd.rstrip()
            table_involved = []
            for tb in dist.remotes:
                if tb in cmd:
                    table_involved.append(tb)

            # check query stats
            # stats = extract_query_meta(cmd)
            # stats.insert(0, indexes[idx])
            # cmd = re.sub(r'^df\d+ = ', '', cmd)

            try:
                start = time.time()
                # append loading table command to the final command
                final_cmd = ''
                input_size = 0
                for tb in table_involved:
                    final_cmd += f"""{tb}=pd.read_sql_table('{tb}', con)\n"""
                final_cmd += cmd
                exec(final_cmd)
                # rs = eval(final_cmd)
                # stats.extend(output_size(rs))
                end = time.time()
                times[idx] = end - start
                print(f'time for [{idx}]  = {end - start}')
            except Exception as e:
                print(e)
                err_count += 1

            # all_query_stats.append(stats)
            writer.writerow([indexes[idx], times[idx]])
            # writer.writerow(stats)
            idx += 1

        # with open(query_stats_log, 'w') as qf:
        #     writer = csv.writer(qf)
        #     writer.writerows(all_query_stats)


if __name__ == "__main__":
    table_list = ['nation', 'customer', 'orders', 'lineitem', 'part', 'partsupp', 'region', 'supplier']
    dbc = DBConfig(db_config)

    from sqlalchemy import create_engine
    con_str = f'postgresql://{dbc.user}:{dbc.passwd}@{dbc.host}:{dbc.port}/{dbc.db}'
    engine = create_engine(con_str)
    con = engine.connect()

    query_path = 'auto_gen_queries'
    for id in [1, 2, 3]:
        random.seed(id*10)
        run_one_dist('p1', table_list, query_path, id, con)



