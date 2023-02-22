import datetime
import marshal
import pickle
import time
import traceback

import psycopg

import aidac
import aidac as pd
import pandas.io.sql as psql
import numpy as np

config = __import__('table_config')

_PATH = getattr(config, 'local_data_path')

date_columns = getattr(config, 'date_columns')
table_dist = getattr(config, 'table_dist')
db_config = getattr(config, 'db_config')


def connect(host, schema, dbname, port, user, pwd):
    aidac.add_data_source('postgres', host, user, pwd, dbname, 'p1', port)


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
    for p in remote_tbs:
        all_tbs[p] = pd.read_remote_data('p1', p)
    return all_tbs

class My_UDF_Class:
    def my_func(self, a, b):
        return a+ b

def udf_func():
    def my_udf(a, b):
        return a+b
    serilaized = marshal.dumps(my_udf.__code__)
    ds = aidac.manager.get_data_source('p1')
    print(serilaized)
    ds._execute('''INSERT INTO my_funcs VALUES ('my_udf', %s)''', serilaized)


def my_query_01():
    """
    140.88
    (q_03) get the revenue for orders before 1995-03-15. order join with lineitem
    plan = (postgres -> (local, postgres))
    @param db:
    @return:
    """
    o = read_file('orders')[['o_orderkey', 'o_orderdate', 'o_shippriority']].head(100)
    l = pd.read_remote_data('p1', 'lineitem')
    t = o.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    t = t.groupby(('l_orderkey', 'o_orderdate', 'o_shippriority')).agg('count')
    return t

def my_query_01_r():
    """
    69.96 (40.60 + 4.41)
    (q_03) get the revenue for orders before 1995-03-15. order join with lineitem
    plan = (postgres -> (local, postgres))
    @param db:
    @return:
    """
    l = read_file('lineitem')
    o = pd.read_remote_data('p1', 'orders')
    t = o.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    t = t.groupby(('l_orderkey', 'o_orderdate', 'o_shippriority')).agg('count')
    return t

def my_query_02():
    """
    209+38"""
    o = read_file('orders')
    l = pd.read_remote_data('p1', 'lineitem')
    l = l.query('l_shipdate < \'1998-9-2\'')
    t = o.merge(l, left_on='o_orderkey', right_on='l_orderkey')[['l_orderkey', 'o_orderdate', 'o_shippriority']]
    return t


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
    tbs = read_tables(locs, remotes)


def mini_05(locs, remotes):
    tbs = read_tables(locs, remotes)
    l = tbs['lineitem']
    my_time = np.datetime64('1995-12-02')
    l = l[l['l_shipdate'] <= my_time]
    l['disc_price'] = l['l_extendedprice'] * (1 - l['l_discount'])
    l = l[['l_returnflag', 'l_linestatus', 'l_quantity', 'l_extendedprice', 'disc_price', 'l_discount']]
    l = l.groupby(['l_returnflag', 'l_linestatus'], sort=False).count()
    return l


def mini_06(locs, remotes):
    start = time.time()
    tbs = read_tables(locs, remotes)
    # print(f'*******read tables={time.time()-start}**********')
    p = tbs['part']
    p = p[(p['p_size'] == 15)]
    if 'part' in locs:
        # todo: series object has no contains
        # p = p[(p['p_size'] == 15) & (p['p_type'].str.contains('^.*BRASS$'))]
        p.query('p_size == 15 and p_type.str.match(\'^.*BRASS$\')')
    p = p[['p_partkey', 'p_mfgr']]
    ps = tbs['partsupp']
    ps = ps[['ps_suppkey', 'ps_supplycost', 'ps_partkey']]
    s = tbs['supplier']
    s = s[['s_nationkey', 's_suppkey', 's_acctbal', 's_name', 's_comment']]
    n = tbs['nation']
    n = n[['n_nationkey', 'n_regionkey', 'n_name']]
    n.materialize()
    print(f'*******1st mat={time.time() - start}**********')
    r = tbs['region']
    r = r[r['r_name'] == 'EUROPE']
    r = r[['r_regionkey']]

    j = ps.merge(s, left_on='ps_suppkey', right_on='s_suppkey')
    j = j.merge(n, left_on='s_nationkey', right_on='n_nationkey')
    j.materialize()
    print(f'*******2nd mat={time.time() - start}**********')
    j = j.merge(r, left_on='n_regionkey', right_on='r_regionkey')

    ti = j[['ps_partkey', 'ps_supplycost']].groupby('ps_partkey').min()

    return ti

def q_02_v1(locs, remotes):
    tbs = read_tables(locs, remotes)
    p = tbs['part']
    ps = tbs['partsupp']
    n = tbs['nation']
    s = tbs['supplier']
    r = tbs['region']

    p = p[p['p_size'] == 15]
    p = p[p['p_type'].str.contains('^.*BRASS$')]
    p = p[['p_partkey', 'p_mfgr']]
    ps = ps[['ps_suppkey', 'ps_supplycost', 'ps_partkey']]

    s = s[['s_nationkey', 's_suppkey', 's_acctbal', 's_name', 's_address', 's_phone', 's_comment']]
    n = n[['n_nationkey', 'n_regionkey', 'n_name']]
    r = r[r['r_name'] == 'EUROPE']
    r = r[['r_regionkey']]

    j = ps.merge(s, left_on='ps_suppkey', right_on='s_suppkey')
    j = j.merge(n, left_on='s_nationkey', right_on='n_nationkey')
    j = j.merge(r, left_on='n_regionkey', right_on='r_regionkey')

    ti = j[['ps_partkey', 'ps_supplycost']].groupby('ps_partkey').min()
    ti.reset_index(inplace=True)
    # ti.rename(columns={'ps_supplycost': 'min_supply_cost', 'ps_partkey': 'i_partkey'}, inplace=True)

    t = j.merge(p, left_on='ps_partkey', right_on='p_partkey')
    t = t.merge(ti, left_on=['ps_partkey', 'ps_supplycost'], right_on=['ps_partkey', 'ps_supplycost'])
    t = t[['s_acctbal', 's_name', 'n_name', 'p_partkey', 'p_mfgr', 's_address', 's_phone', 's_comment']]
    t.sort_values(['s_acctbal', 'n_name', 's_name', 'p_partkey'], ascending=[False, True, True, True])

    return t

def q_03_v1(locs, remotes):
    tbs = read_tables(locs, remotes)
    c = tbs['customer']
    o = tbs['orders']
    l = tbs['lineitem']
    c = c[c['c_mktsegment'] == 'BUILDING']['c_custkey']
    o = o[o['o_orderdate'] < np.datetime64('1995-03-15')]
    o = o[['o_orderdate', 'o_shippriority', 'o_orderkey', 'o_custkey']]
    l = l[l['l_shipdate'] > np.datetime64('1995-03-15')]
    l['revenue'] = l['l_extendedprice'] * (1 - l['l_discount'])
    l = l[['l_orderkey', 'revenue']]

    t = c.merge(o, left_on='c_custkey', right_on='o_custkey', how='inner')
    t = t.merge(l, left_on='o_orderkey', right_on='l_orderkey', how='inner')
    t = t[['l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority']]
    t = t.groupby(['l_orderkey', 'o_orderdate', 'o_shippriority'], sort=False).agg({'revenue': 'sum'})
    #print(t.genSQL)
    return t

def q_04_v1(locs, remotes):
    tbs = read_tables(locs, remotes)
    l = tbs['lineitem']
    o = tbs['orders']

    l = l[l['l_commitdate'] < l['l_receiptdate']]
    o = o[(o['o_orderdate'] >= np.datetime64('1993-07-01'))
          &(o['o_orderdate'] < np.datetime64('1993-10-01'))]
    t = o.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    t = t[['o_orderpriority', 'o_orderkey']]

    t = t.groupby('o_orderpriority').count()
    t.sort_values('o_orderpriority')
    return t


def q_05_v1(locs, remotes):
    tbs = read_tables(locs, remotes)
    c = tbs['customer']
    o = tbs['orders']
    l = tbs['lineitem']
    s = tbs['supplier']
    n = tbs['nation']
    r = tbs['region']

    c = c[['c_custkey', 'c_nationkey']]

    s = s[['s_nationkey', 's_suppkey']]
    n = n[['n_name', 'n_nationkey', 'n_regionkey']]

    o = o[(o['o_orderdate'] >= datetime.date(1994, 1, 1))
         &(o['o_orderdate'] < datetime.date(1995, 1, 1))]
    o = o[['o_orderkey', 'o_custkey']]
    l = l[['l_suppkey', 'l_orderkey', 'l_discount', 'l_extendedprice']]
    r = r[r['r_name'] == 'ASIA']
    r = r[['r_regionkey']]

    t = c.merge(o, left_on='c_custkey', right_on='o_custkey')
    t = t.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    t = t.merge(s, left_on=['l_suppkey', 'c_nationkey'], right_on=['s_suppkey', 's_nationkey'])
    t = t.merge(n, left_on='s_nationkey', right_on='n_nationkey')
    t = t.merge(r, left_on='n_regionkey', right_on='r_regionkey')
    t['revenue'] = t['l_extendedprice'] * (1 - t['l_discount'])
    t = t[['n_name', 'revenue']]
    t = t.groupby(('n_name'), sort=False).sum()
    t.reset_index(inplace=True)
    t.sort_values('revenue', ascending=False)

    return t;
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
    #print(t.genSQL)
    return t

def q_13_v1(locs, remotes):
    tbs = read_tables(locs, remotes)
    c = tbs['customer']
    c = c[['c_custkey']]
    o = tbs['orders']
    o = o[o['o_comment'].str.contains('^.*special.*requests.*$', regex=True)]
    o = o[['o_orderkey', 'o_custkey']]
    t = c.merge(o, left_on='c_custkey', right_on='o_custkey', how='left')
    t = t[['c_custkey', 'o_orderkey']]
    t = t.groupby('c_custkey').count()
    t.reset_index(inplace=True)
    t = t.groupby('o_orderkey').count()
    t.reset_index(inplace=True)
    t.sort_values(['c_custkey', 'o_orderkey'], ascending=[False, False])
    #print(t.genSQL)
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
    t = 100 * t['revenue1'] / t['revenue2']
    # t = pd.DataFrame({'promo_revenue': [t]})
    return t


def q_15_v1(locs, remotes):
    tbs = read_tables(locs, remotes)
    l = tbs['lineitem']
    l = l[['l_partkey', 'l_quantity', 'l_extendedprice']]
    p = tbs['part']
    p = p[['p_partkey', 'p_brand', 'p_container']]

    ti = l.merge(p, left_on='l_partkey', right_on='p_partkey')
    # todo: having t=ti here cause datasource to be none
    t = ti
    ti = ti[['p_partkey', 'l_quantity']].head(200)
    ti = ti.groupby('p_partkey').mean()
    ti.reset_index(inplace=True)
    ti['avg_qty'] = ti['l_quantity'] * 0.2
    ti = ti[['p_partkey', 'avg_qty']]
    ti.materialize()
    print(ti.data)

    t = t[(t['p_brand'] == 'Brand#23') & (t['p_container'] == 'MED BOX')]
    t = t.merge(ti, left_on='p_partkey', right_on='p_partkey')
    t = t[t['l_quantity'] < t['avg_qty']]
    t.materialize()
    print(t.data)
    t = t[['l_extendedprice']]
    t = t.sum()
    return t


def random_01(locs, remotes):
    tbs = read_tables(locs, remotes)
    partsupp = tbs['partsupp']
    lineitem = tbs['lineitem']
    supplier = tbs['supplier']
    df1112 = partsupp[partsupp['ps_supplycost'] >= 863].merge(
        lineitem[(lineitem['l_suppkey'] <= 6543) | (lineitem['l_quantity'] <= 43)].merge(
            partsupp[partsupp['ps_supplycost'] >= 149].merge(
                supplier[(supplier['s_nationkey'] >= 0) | (supplier['s_acctbal'] >= 2154)], left_on='ps_suppkey',
                right_on='s_suppkey'), left_on='l_suppkey', right_on='s_suppkey'), left_on='ps_suppkey',
        right_on='s_suppkey')
    df1113 = partsupp[partsupp['ps_supplycost'] >= 863].merge(
        lineitem[(lineitem['l_suppkey'] <= 6543) | (lineitem['l_quantity'] <= 43)].merge(
            partsupp[partsupp['ps_supplycost'] >= 149].merge(
                supplier[(supplier['s_nationkey'] < 19) | (supplier['s_acctbal'] < 11581)][['s_suppkey', 's_phone']],
                left_on='ps_suppkey', right_on='s_suppkey'), left_on='l_suppkey', right_on='s_suppkey'),
        left_on='ps_suppkey', right_on='s_suppkey')
    return df1113


def measure_time(func, *args):
    start = time.time()
    rs = func(*args)
    # print('func called')
    print(rs.genSQL)
    rs.materialize()
    # print('materialized')
    end = time.time()
    print(rs.data)
    print('Function {} takes time {}'.format(func, end-start))


if __name__ == '__main__':
    connect(db_config['host'], db_config['schema'], db_config['db'], db_config['port'], db_config['user'],
                 db_config['passwd'])
    full_qry = ['mini_03', 'mini_05', 'mini_06', 'q_02_v1', 'q_03_v1', 'q_04_v1', 'q_05_v1', 'q_10_v1', 'q_13_v1', 'q_14_v1', 'q_15_v1']
    qrys = ['q_02_v1', 'q_03_v1', 'q_04_v1', 'q_05_v1', 'q_10_v1', 'q_13_v1', 'q_14_v1', 'q_15_v1']
    for q in qrys:
        for ls, rs in table_dist[q]:
            try:
                print('\n----------------------------------------------\n'
                      'test qry {}, locals: {}, remotes: {}\n'
                      '---------------------------------------------'.format(q, ls, rs))
                measure_time(locals()[q], ls, rs)
            except Exception as e:
                traceback.print_exc()



"""
SELECT (revenue2/(100-revenue1)) AS revenue1 FROM (SELECT revenue1 FROM (SELECT revenue2 , sum(revenue1) AS revenue1 FROM (SELECT l_partkey AS l_partkey, l_extendedprice AS l_extendedprice, l_discount AS l_discount, p_partkey AS p_partkey, p_type AS p_type, ((1-l_discount)*l_extendedprice) AS revenue2, (5+revenue2) AS revenue1 FROM (SELECT l_partkey AS l_partkey, l_extendedprice AS l_extendedprice, l_discount AS l_discount, p_partkey AS p_partkey, p_type AS p_type, ((1-l_discount)*l_extendedprice) AS revenue2 FROM (SELECT SQLProjectionTransform76.l_partkey AS l_partkey, SQLProjectionTransform76.l_extendedprice AS l_extendedprice, SQLProjectionTransform76.l_discount AS l_discount, SQLProjectionTransform77.p_partkey AS p_partkey, SQLProjectionTransform77.p_type AS p_type FROM (SELECT l_partkey AS l_partkey, l_extendedprice AS l_extendedprice, l_discount AS l_discount FROM (SELECT l_comment AS l_comment, l_commitdate AS l_commitdate, l_discount AS l_discount, l_extendedprice AS l_extendedprice, l_linenumber AS l_linenumber, l_linestatus AS l_linestatus, l_orderkey AS l_orderkey, l_partkey AS l_partkey, l_quantity AS l_quantity, l_receiptdate AS l_receiptdate, l_returnflag AS l_returnflag, l_shipdate AS l_shipdate, l_shipinstruct AS l_shipinstruct, l_shipmode AS l_shipmode, l_suppkey AS l_suppkey, l_tax AS l_tax FROM (SELECT * FROM lineitem) lineitem WHERE (l_shipdate >= '1995-09-01 00:00:00') AND (l_shipdate < '1995-10-01 00:00:00')) SQLProjectionTransform75) SQLProjectionTransform76 INNER JOIN (SELECT p_partkey AS p_partkey, p_type AS p_type FROM (SELECT * FROM part) part) SQLProjectionTransform77 ON SQLProjectionTransform76.l_partkey = SQLProjectionTransform77.p_partkey) SQLJoinTransform78) SQLProjectionTransform83)SQLProjectionTransform86 
GROUP BY revenue2 ORDER BY revenue2) SQLAGG_Transform88) SQLBinaryOperationTransform90
"""