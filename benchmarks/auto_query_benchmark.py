import random
import time
import sys

import aidac
import csv

from benchmarks.dbc import *

config = __import__('table_config')

_PATH = getattr(config, 'local_data_path')

date_columns = getattr(config, 'date_columns')
db_config = getattr(config, 'db_config')

with_materialization = False
use_existing_meta = True

large_rs = [2986, 2981, ]
# todo
"""# r_comment
nation.merge(region[region['r_regionkey'] == 2].merge(supplier.merge(region[region['r_regionkey'] > 2][['r_regionkey','r_name','r_comment']].groupby(by=['r_comment']).agg('count').merge(nation[nation['n_nationkey'] < 14][['n_nationkey','n_name','n_regionkey','n_comment']].groupby(by=['n_comment']).agg('max', numeric_only=True),left_on='r_regionkey', right_on='n_regionkey'),left_on='s_nationkey', right_on='n_nationkey'),left_on='r_regionkey', right_on='n_regionkey'),left_on='n_nationkey', right_on='s_nationkey')[['n_regionkey_y','r_comment','r_regionkey_x','s_nationkey','n_nationkey_x','s_name','n_comment','r_name','r_regionkey_y','s_phone','n_regionkey_x']].materialize()
"""

query = "df2622=lineitem[(lineitem['l_extendedprice'] >= 34779.68) | (lineitem['l_linenumber'] != 3) | (lineitem['l_discount'] != 0.0) & (lineitem['l_orderkey'] == 120)].merge(supplier[(supplier['s_acctbal'] > 1089.16) | (supplier['s_acctbal'] <= 1768.16)][['s_suppkey','s_address','s_phone','s_acctbal']].groupby(by=['s_address']).agg('mean', numeric_only=True),left_on='l_suppkey', right_on='s_suppkey')[['l_linestatus','l_tax','l_orderkey','l_returnflag','l_commitdate','s_acctbal','l_partkey','l_receiptdate','s_suppkey','l_shipmode','l_linenumber','l_extendedprice','l_shipinstruct','l_suppkey','l_shipdate','l_quantity','l_comment']]"

def read_auto_gen_queries(path, mat=False, indexes=None):
    if mat:
        queries = []
        for idx in indexes:
            with open(path+f'{idx}.txt') as f:
                queries.append('\n'.join(f.readlines()))
        return queries
    with open(path, 'r') as f:
        return f.readlines()


def run_one_dist(job, table_list, rpath, wpath, wlogpath):
    if use_existing_meta:
        specs = read_specs(wlogpath)
        dist = Dist(job, table_list, specs[0], specs[1])
    else:
        dist = Dist(job, table_list)

    nation = dist.nation
    customer = dist.customer
    orders = dist.orders
    lineitem = dist.lineitem
    part = dist.part
    partsupp = dist.partsupp
    region = dist.region
    supplier = dist.supplier

    # start index, end index and step for iterating the queries
    sc, ec, step = 0, 3000, 6
    # total query count and number of errors
    idx, err_count = 0, 0

    indexes = range(sc, ec, step)
    if with_materialization:
        queries = read_auto_gen_queries(rpath, with_materialization, indexes)
    else:
        queries = read_auto_gen_queries(rpath, with_materialization, indexes)[sc:ec:step]
    times = [0] * len(queries)
    # for cmd in queries:
        # append materialize command at the end
    cmd = query.rstrip() + '.materialize()'
    # if idx in large_rs:
    #     continue
    try:
        start = time.time()
        exec(cmd)
        end = time.time()
        times[idx] = end - start
        print(f'time for [{idx}]  = {end - start}')
    except Exception as e:
        print(cmd)
        print(e)
        err_count += 1
    idx += 1

    # write to result file
    # with open(wpath, 'w') as f:
    #     writer = csv.writer(f)
    #     writer.writerows(zip(indexes, times))
    #
    # # write general information and table location for reproduction
    # with open(wlogpath, 'w') as f:
    #     lines = [
    #         dist.table_loc()+'\n'
    #         f'Start index = {sc}, end index = {ec}, step = {step}\n',
    #         f'total count = {idx}, error_count = {err_count}\n'
    #     ]
    #     f.writelines(lines)


if __name__ == "__main__":
    table_list = ['nation', 'customer', 'orders', 'lineitem', 'part', 'partsupp', 'region', 'supplier']
    dbc = DBConfig(db_config)
    aidac.add_data_source('postgres', dbc.host, dbc.user, dbc.passwd, dbc.db, 'p1', dbc.port)

    query_path = 'auto_gen_queries'

    for id in range(1, 2):
        random.seed(id*10)
        if with_materialization:
            rpath = f'{query_path}/auto_materialization/'
            wpath = f'{query_path}/out_{id}_mat.txt'
            wlogpath = f'{query_path}/out_{id}_meta.txt'
            std_out_log = f'{query_path}/out_{id}_stdout_mat.txt'
        else:
            rpath = f'{query_path}/merged_queries_auto_sf0001.txt'
            wpath = f'{query_path}/out_{id}.txt'
            wlogpath = f'{query_path}/out_{id}_meta.txt'
            std_out_log = f'{query_path}/out_{id}_stdout.txt'

        # with open(std_out_log, 'w') as sys.stdout:
        run_one_dist('p1', table_list, rpath, wpath, wlogpath)


