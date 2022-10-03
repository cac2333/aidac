import datetime
import time
import traceback

import psycopg

import pandas as pd
import pandas.io.sql as psql

import numpy as np

config = __import__('table_config')


_PATH = getattr(config, 'local_data_path')
date_columns = getattr(config, 'date_columns')
table_dist = getattr(config, 'table_dist')
db_config = getattr(config, 'db_config')

class Database:
    def __init__(self, host, schema, dbname, port, user, pwd):
        self.con = psycopg.connect(
            '''host={} 
            dbname={} 
            user={} 
            port={} 
            password={}'''.format(host, dbname, user, port, pwd)
        )
        self.schema = schema

    def setBufferSize(self, size):
        self.con.replysize = size

    def get_table(self, name):
        t = pd.DataFrame(psql.read_sql_query('SELECT * FROM {0}.{1};'.format(self.schema, name), self.con))
        print(f'load table {name}: size={len(t)*len(t.columns)}')
        return t


def read_file(table, parse_dates=False):
    return pd.read_csv(''+_PATH+table+'.csv', parse_dates=parse_dates)


def date_conversion(t, name):
    if name in table_dist:
        for id, col in enumerate(t.columns):
            for date_id in table_dist[name]:
                if id == date_id:
                    t[col] = pd.to_datetime(t[col]).dt.date
    return t


def read_tables(local_tbs, remote_tbs):
    all_tbs = {}
    for p in local_tbs:
        if p in date_columns:
            all_tbs[p] = read_file(p, parse_dates=date_columns[p])
        else:
            all_tbs[p] = read_file(p)
    for p in remote_tbs:
        all_tbs[p] = my_db.get_table(p)

    for tb in all_tbs:
        all_tbs[tb] = date_conversion(all_tbs[tb], tb)
    return all_tbs


def mini_01(locs, remotes):
    tbs = read_tables(locs, remotes)
    l = tbs['lineitem']
    return l


def mini_02(locs, remotes):
    tbs = read_tables(locs, remotes)
    o = tbs['orders']
    return o


def mini_03(locs, remotes):
    tbs = read_tables(locs, remotes)
    l = tbs['lineitem']
    o = tbs['orders'].query('o_orderpriority==\'1-URGENT\'')
    t = o.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    return t


def mini_04(locs, remotes):
    pass


def mini_05(locs, remotes):
    tbs = read_tables(locs, remotes)
    l = tbs['lineitem']
    my_date = np.datetime64('1995-12-02')
    l = l[l['l_shipdate'] <= my_date]
    l['disc_price'] = l['l_extendedprice'] * (1 - l['l_discount'])
    l = l[['l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'disc_price', 'l_discount']]
    l = l.groupby(['l_returnflag', 'l_linestatus'], sort=False).count()
    return l


def mini_06(locs, remotes):
    tbs = read_tables(locs, remotes)
    p = tbs['part']
    p = p[(p['p_size'] == 15) & (p['p_type'].str.contains('^.*BRASS$'))]
    p = p[['p_partkey', 'p_mfgr']]
    ps = tbs['partsupp']
    ps = ps[['ps_suppkey', 'ps_supplycost', 'ps_partkey']]
    s = tbs['supplier']
    s = s[['s_nationkey', 's_suppkey', 's_acctbal', 's_name', 's_comment']]
    n = tbs['nation']
    n = n[['n_nationkey', 'n_regionkey', 'n_name']]
    r = tbs['region']
    r = r[r['r_name'] == 'EUROPE']
    r = r[['r_regionkey']]

    j = ps.merge(s, left_on='ps_suppkey', right_on='s_suppkey')
    j = j.merge(n, left_on='s_nationkey', right_on='n_nationkey')
    j = j.merge(r, left_on='n_regionkey', right_on='r_regionkey')

    ti = j[['ps_partkey', 'ps_supplycost']].groupby('ps_partkey').min()

    return ti

def q_01_v1():
    """291"""
    l = pd.read_remote_data('p1', 'lineitem')
    l = l.query('l_shipdate <= \'1998-9-2\'')
    l.sort_values(['l_returnflag', 'l_linestatus'])
    return l


def q_01_v2(db):
    l = db.lineitem
    l = l[l['l_shipdate'] <= np.datetime64('1998-09-02')]
    l['disc_price'] = l['l_extendedprice'] * (1 - l['l_discount'])
    l['charge'] = l['disc_price'] * (1 + l['l_tax'])
    l = l[['l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'disc_price', 'charge', 'l_discount']]
    l = l.groupby(('l_returnflag', 'l_linestatus'), sort=False) \
        .agg({'l_quantity': ['sum', 'mean'], 'l_extendedprice': ['sum', 'mean'], 'disc_price': 'sum',
        'charge': 'sum', 'l_discount': ['mean', 'count']})
    l.columns = pd.Index([l.columns.levels[1][l.columns.labels[1][i]] + '_' + l.columns.levels[0][l.columns.labels[0][i]] for i in range(len(l.columns.labels[0]))])
    l.rename(columns={'sum_l_quantity': 'sum_qty', 'sum_l_extendedprice': 'sum_base_price',
        'mean_l_quantity': 'avg_qty', 'mean_l_extendedprice': 'avg_price',
        'mean_l_discount': 'avg_disc', 'count_l_discount': 'count_order'}, inplace=True)
    l.reset_index(inplace=True)
    l.sort_values(['l_returnflag', 'l_linestatus'], inplace=True)
    return l


def q_03_v1(locs, remotes):
    tbs = read_tables(locs, remotes)
    c = tbs['customer']
    o = tbs['orders']
    l = tbs['lineitem']
    c = c[c['c_mktsegment'] == 'BUILDING']
    c = c[['c_custkey']]
    print(o.dtypes)
    o = o[o['o_orderdate'] < np.datetime64('1995-03-15')]
    o = o[['o_orderdate', 'o_shippriority', 'o_orderkey', 'o_custkey']]
    l = l[l['l_shipdate'] > np.datetime64('1995-03-15')]
    l['revenue'] = l['l_extendedprice'] * (1 - l['l_discount'])
    l = l[['l_orderkey', 'revenue']]

    t = c.merge(o, left_on='c_custkey', right_on='o_custkey', how='inner')
    t = t.merge(l, left_on='o_orderkey', right_on='l_orderkey', how='inner')
    t = t[['l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority']]
    t = t.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority'], sort=False).agg({'revenue': 'sum'})
    return t


def q_10_v1(locs, remotes):
    tbs = read_tables(locs, remotes)

    c = tbs['customer']
    c = c[['c_custkey', 'c_nationkey', 'c_name', 'c_acctbal', 'c_address', 'c_phone', 'c_comment']]
    o = tbs['orders']
    o = o[(o['o_orderdate'] >= np.datetime64('1993-10-01'))
         &(o['o_orderdate'] < np.datetime64('1994-01-01'))]
    o = o[['o_orderkey', 'o_custkey']]
    l = tbs['lineitem']
    l = l[l['l_returnflag'] == 'R']
    l['revenue'] = l['l_extendedprice'] * (1 - l['l_discount'])
    l = l[['l_orderkey', 'revenue']]
    n = tbs['nation']
    n = n[['n_name', 'n_nationkey']]

    t = c.merge(o, left_on='c_custkey', right_on='o_custkey')
    t = t.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    t = t.merge(n, left_on='c_nationkey', right_on='n_nationkey')
    t = t[['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment', 'revenue']]
    t = t.groupby(['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment']).agg({'revenue': 'sum'})
    t.sort_values('revenue', ascending=False)
    return t

def q_13_v1(locs, remotes):
    tbs = read_tables(locs, remotes)
    c = tbs['customer']
    c = c[['c_custkey']]
    o = tbs['orders']
    o = o[o['o_comment'].str.contains('^.*special.*requests.*$')]
    o = o[['o_orderkey', 'o_custkey']]
    t = c.merge(o, left_on='c_custkey', right_on='o_custkey', how='left')
    t = t[['c_custkey', 'o_orderkey']]
    t = t.groupby('c_custkey').count()
    t.reset_index(inplace=True)
    t = t.groupby('o_orderkey').count()
    t.reset_index(inplace=True)
    t.sort_values(['c_custkey', 'o_orderkey'], ascending=[False, False])
    return t


def q_14_v1(locs, remotes):
    tbs = read_tables(locs, remotes)
    l = tbs['lineitem']
    l = l[(l['l_shipdate'] >= datetime.datetime(1995, 9, 1))
         &(l['l_shipdate'] < datetime.datetime(1995, 10, 1))]
    l = l[['l_partkey', 'l_extendedprice', 'l_discount']]
    p = tbs['part']
    p = p[['p_partkey', 'p_type']]

    t = l.merge(p, left_on='l_partkey', right_on='p_partkey')
    t['revenue2'] = t['l_extendedprice'] * (1 - t['l_discount'])
    t['revenue1'] = t['revenue2']+5
    # todo: solve unsupported column types
    t = t.groupby('revenue2').agg({'revenue2': ['sum']})
    t = 100 * t['revenue1'] / t['revenue2']
    return t;

def q_15_v1(locs, remotes):
    tbs = read_tables(locs, remotes)
    s = tbs['supplier']
    s = s[['s_suppkey', 's_name', 's_address', 's_phone']]
    l = tbs['lineitem']
    l = l[(l['l_shipdate'] >= datetime.date(1996, 1, 1))
         &(l['l_shipdate'] < datetime.date(1996, 4, 1))]
    l['total_revenue'] = l['l_extendedprice'] * (1 - l['l_discount'])
    l = l[['l_suppkey', 'total_revenue']]
    l = l.groupby('l_suppkey').sum(min_count=1)
    l.reset_index(inplace=True)

    ti = l.agg(max)

    t = s.merge(l, left_on='s_suppkey', right_on='l_suppkey')
    t = t[t['total_revenue'] == ti['total_revenue']]
    t = t[['s_suppkey', 's_name', 's_address', 's_phone', 'total_revenue']]
    t.sort_values('s_suppkey', inplace=True)


def measure_time(func, *args):
    start = time.time()
    rs = func(*args)
    end = time.time()
    print(rs)
    print('Function {} takes time {}'.format(func, end-start))


my_db = Database(db_config['host'], db_config['schema'], db_config['db'], db_config['port'], db_config['user'],
                 db_config['passwd'])


if __name__ == '__main__':
    full_qry = ['mini_01', 'mini_02', 'mini_03', 'mini_05', 'mini_06', 'q_03_v1', 'q_10_v1', 'q_13_v1']
    qrys = ['mini_03']
    for q in full_qry:
        try:
            for ls, rs in table_dist[q]:
                print('----------------------------------------------\n'
                      'test qry {}, locals: {}, remotes: {}\n'
                      '---------------------------------------------'.format(q, ls, rs))
                measure_time(locals()[q], ls, rs)
        except Exception as e:
            traceback.print_exc()