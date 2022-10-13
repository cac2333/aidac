import datetime
import time

import numpy as np
import psycopg
import pandas as pd

config = __import__('table_config')

date_columns = getattr(config, 'date_columns')
table_dist = getattr(config, 'table_dist')
db_config = getattr(config, 'db_config')

_PATH = getattr(config, 'local_data_path')


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

    def read_sql(self, sql):
        t = pd.read_sql_query(sql, self.con)
        return t


def read_file(table, parse_dates=False):
    return pd.read_csv(''+_PATH+table+'.csv', parse_dates=parse_dates)


def read_tables(local_tbs, remote_tbs, sqls):
    all_tbs = {}
    for p in local_tbs:
        if p in date_columns:
            all_tbs[p] = read_file(p, parse_dates=date_columns[p])
        else:
            all_tbs[p] = read_file(p)
    for p in remote_tbs:
        if p in sqls:
            sql = sqls[p]
        else:
            sql = f'SELECT * FROM {p};'
        all_tbs[p] = my_db.read_sql(sql)
    return all_tbs


def mini_01(locs, remotes):
    sqls = {'lineitem': """SELECT * FROM lineitem"""}
    tbs = read_tables(locs, remotes, sqls)
    l = tbs['lineitem']
    return l


def mini_02(locs, remotes):
    sqls = {'orders': """SELECT * FROM orders"""}
    tbs = read_tables(locs, remotes, sqls)
    l = tbs['orders']
    return l


"""
SELECT * FROM lineitem, orders WHERE o_orderpriority='1-URGENT' AND o_orderkey=l_orderkey
"""
def mini_03(locs, remotes):
    sqls = {'lineitem': """SELECT * FROM lineitem""",
            'orders': """SELECT * FROM orders WHERE o_orderpriority=\'1-URGENT\'"""}
    tbs = read_tables(locs, remotes, sqls)
    l = tbs['lineitem']
    o = tbs['orders'].query('o_orderpriority==\'1-URGENT\'')
    t = o.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    return t


def mini_05(locs, remotes):
    sqls = {
        'lineitem': """
        SELECT COUNT(*) FROM (SELECT l_returnflag, l_linestatus,l_quantity, l_extendedprice, l_discount,l_extendedprice * (1 - l_discount) AS disc_price FROM lineitem where
    l_shipdate <= date '1995-12-2') proj GROUP BY l_returnflag, l_linestatus;"""
    }
    tbs = read_tables(locs, remotes, sqls)
    l = tbs['lineitem']
    if 'lineitem' in locs:
        l = l[l['l_shipdate'] <= np.datetime64('1995-12-02')]
        l['disc_price'] = l['l_extendedprice'] * (1 - l['l_discount'])
        l = l[['l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'disc_price', 'l_discount']]
        l = l.groupby(['l_returnflag', 'l_linestatus'], sort=False).count()
    return l


def mini_06(locs, remotes):
    sqls = {
        'part': """SELECT p_partkey, p_mfgr FROM part WHERE p_size=15 AND p_type ~ \'^.*BRASS$\'""",
        'partsupp': """SELECT ps_suppkey, ps_supplycost, ps_partkey FROM partsupp""",
        'supplier': """SELECT s_nationkey, s_suppkey, s_acctbal, s_name, s_comment FROM supplier"""
    }
    tbs = read_tables(locs, remotes, sqls)

    p = tbs['part']
    if 'part' in locs:
        p = p[(p['p_size'] == 15) & (p['p_type'].str.contains('^.*BRASS$'))]
        p = p[['p_partkey', 'p_mfgr']]

    ps = tbs['partsupp']
    if 'partsupp' in locs:
        ps = ps[['ps_suppkey', 'ps_supplycost', 'ps_partkey']]
    print(ps)

    s = tbs['supplier']
    if 'supplier' in locs:
        s = s[['s_nationkey', 's_suppkey', 's_acctbal', 's_name', 's_comment']]

    n = tbs['nation']
    n = n[['n_nationkey', 'n_regionkey', 'n_name']]
    r = tbs['region']
    r = r[r['r_name'].str.contains('EUROPE')]
    r = r[['r_regionkey']]

    j = ps.merge(s, left_on='ps_suppkey', right_on='s_suppkey')
    j = j.merge(n, left_on='s_nationkey', right_on='n_nationkey')
    j = j.merge(r, left_on='n_regionkey', right_on='r_regionkey')

    ti = j[['ps_partkey', 'ps_supplycost']].groupby('ps_partkey').min()
    print(ti)
    return ti

def q_03_v1(locs, remotes):
    sqls = {
        'customer': """SELECT c_custkey FROM customer WHERE c_mktsegment=\'BUILDING\'""",
        'orders': """SELECT o_orderdate, o_shippriority, o_orderkey, o_custkey 
        FROM orders WHERE o_orderdate< date '1995-3-15'""",
        'lineitem': """SELECT l_orderkey, l_extendedprice*(1-l_discount) AS revenue FROM lineitem WHERE 
        l_shipdate > date '1995-3-15'"""
    }
    tbs = read_tables(locs, remotes, sqls)
    c = tbs['customer']
    o = tbs['orders']
    l = tbs['lineitem']
    if 'customer' in locs:
        c = c[c['c_mktsegment'] == 'BUILDING']
        c = c[['c_custkey']]

    if 'orders' in locs:
        o = o[o['o_orderdate'] < np.datetime64('1995-03-15')]
        o = o[['o_orderdate', 'o_shippriority', 'o_orderkey', 'o_custkey']]

    if 'lineitem' in locs:
        l = l[l['l_shipdate'] > np.datetime64('1995-03-15')]
        l['revenue'] = l['l_extendedprice'] * (1 - l['l_discount'])
        l = l[['l_orderkey', 'revenue']]

    t = c.merge(o, left_on='c_custkey', right_on='o_custkey', how='inner')
    t = t.merge(l, left_on='o_orderkey', right_on='l_orderkey', how='inner')
    t = t[['l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority']]
    print(t)
    t = t.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority'], sort=False).agg({'revenue': 'sum'})
    return t


def q_10_v1(locs, remotes):
    sqls = {
        'orders': """SELECT o_orderkey, o_custkey FROM orders WHERE o_orderdate >= date \'1993-10-01\'
                and o_orderdate < date \'1994-01-01\'""",
        'customer': """
            SELECT c_custkey, c_nationkey, c_name, c_acctbal, c_address, c_phone, c_comment FROM customer
        """,
        'lineitem': """
            SELECT l_extendedprice * (1 - l_discount) as revenue, l_orderkey FROM lineitem WHERE l_returnflag = 'R'
        """,
        'nation': """SELECT n_name, n_nationkey FROM nation"""
        }
    tbs = read_tables(locs, remotes, sqls)

    c = tbs['customer']
    if 'customer' in locs:
        c = c[['c_custkey', 'c_nationkey', 'c_name', 'c_acctbal', 'c_address', 'c_phone', 'c_comment']]

    o = tbs['orders']
    if 'orders' in locs:
        o = o[(o['o_orderdate'] >= np.datetime64('1993-10-01'))
             &(o['o_orderdate'] < np.datetime64('1994-01-01'))]
        o = o[['o_orderkey', 'o_custkey']]

    l = tbs['lineitem']
    if 'lineitem' in locs:
        l = l[l['l_returnflag'] == 'R']
        l['revenue'] = l['l_extendedprice'] * (1 - l['l_discount'])
        l = l[['l_orderkey', 'revenue']]

    n = tbs['nation']
    if 'nation' in locs:
        n = n[['n_name', 'n_nationkey']]

    t = c.merge(o, left_on='c_custkey', right_on='o_custkey')
    t = t.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    t = t.merge(n, left_on='c_nationkey', right_on='n_nationkey')
    t = t[['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment', 'revenue']]
    t = t.groupby(['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment']).agg({'revenue': 'sum'})
    t.sort_values('revenue', ascending=False)
    return t


def q_13_v1(locs, remotes):
    sqls = {
        'customer': """SELECT c_custkey FROM customer""",
        'orders': """SELECT o_orderkey, o_custkey FROM orders WHERE o_comment ~ '^.*special.*requests.*$'"""
    }
    tbs = read_tables(locs, remotes, sqls)
    c = tbs['customer']
    if 'customer' in locs:
        c = c[['c_custkey']]
    o = tbs['orders']
    if 'orders' in locs:
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

def q_15_v1(locs, remotes):
    sqls = {
        'lineitem': """SELECT l_partkey, l_quantity, l_extendedprice from lineitem""",
        'part': """SELECT p_partkey, p_brand, p_container from part"""
    }

    tbs = read_tables(locs, remotes, sqls)
    l = tbs['lineitem']
    if 'lineitem' in sqls:
        l = l[['l_partkey', 'l_quantity', 'l_extendedprice']]

    p = tbs['part']
    if 'part' in sqls:
        p = p[['p_partkey', 'p_brand', 'p_container']]

    ti = l.merge(p, left_on='l_partkey', right_on='p_partkey')
    t = ti
    ti = ti[['p_partkey', 'l_quantity']]
    ti.materialize()
    ti = ti.groupby('p_partkey').mean()
    ti['avg_qty'] = ti['l_quantity'] * 0.2
    ti.reset_index(inplace=True)
    ti = ti[['p_partkey', 'avg_qty']]

    t = t[(t['p_brand'] == 'Brand#23') & (t['p_container'] == 'MED BOX')]
    t = t.merge(ti, left_on='p_partkey', right_on='p_partkey')
    t = t[t['l_quantity'] < t['avg_qty']]
    t.materialize()
    t = t[['l_extendedprice']]
    t = t.sum()
    return t

def measure_time(func, *args):
    start = time.time()
    rs = func(*args)
    end = time.time()
    print(rs)
    print('Function {} takes time {}'.format(func, end-start))


my_db = Database(db_config['host'], db_config['schema'], db_config['db'], db_config['port'], db_config['user'],
                 db_config['passwd'])


if __name__ == '__main__':
    qrys = ['mini_06']
    for q in qrys:
        for ls, rs in table_dist[q]:
            print('----------------------------------------------\n'
                  'test qry {}, locals: {}, remotes: {}\n'
                  '---------------------------------------------'.format(q, ls, rs))
            measure_time(locals()[q], ls, rs)