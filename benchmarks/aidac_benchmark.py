import datetime
import marshal
import pickle
import time

import psycopg

import aidac
import aidac as pd
import pandas.io.sql as psql

_PATH = 'datasets/'


def connect(host, dbname, schema, port, user, pwd):
    aidac.add_data_source('postgres', host, user, pwd, dbname, 'p1', port)


def read_file(table, parse_dates=False):
    return pd.read_csv(_PATH+table+'.csv', parse_dates=parse_dates)

class My_UDF_Class:
    def my_func(self, a, b):
        return a+ b

def udf_func():
    import dill
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

def q_01_v1():
    l = pd.read_remote_data('p1', 'lineitem')
    l = l.query('l_shipdate <= \'1998-9-2\'')
    l.sort_values(['l_returnflag', 'l_linestatus'])
    return l

def q_01_v2():
    """209+38"""
    o = read_file('orders')
    l = pd.read_remote_data('p1', 'lineitem')
    l = l.query('l_shipdate < \'1998-9-2\'')
    t = o.merge(l, left_on='o_orderkey', right_on='l_orderkey')[['l_orderkey', 'o_orderdate', 'o_shippriority']]
    return t

def q_02_v1():
    p = pd.read_remote_data('p1', 'part')
    p = p[(p['p_size'] == 15) & (p['p_type'].str.contains('^.*BRASS$'))]
    p = p[['p_partkey', 'p_mfgr']]
    ps = pd.read_remote_data('p1', 'partsupp')
    ps = ps[['ps_suppkey', 'ps_supplycost', 'ps_partkey']]
    s = read_file('supplier')
    s = s[['s_nationkey', 's_suppkey', 's_acctbal', 's_name', 's_address', 's_phone', 's_comment']]
    n = pd.read_remote_data('p1', 'nation')
    n = n[['n_nationkey', 'n_regionkey', 'n_name']]
    r = read_file('region')
    r = r[r['r_name'] == 'EUROPE']
    r = r[['r_regionkey']]

    j = ps.merge(s, left_on='ps_suppkey', right_on='s_suppkey')
    j = j.merge(n, left_on='s_nationkey', right_on='n_nationkey')
    j = j.merge(r, left_on='n_regionkey', right_on='r_regionkey')

    ti = j[['ps_partkey', 'ps_supplycost']].groupby('ps_partkey').min()
    print(ti.genSQL)
    return ti


def q_03_v1():
    c = pd.read_remote_data('p1', 'customer')
    o = read_file('orders', parse_dates=[4])
    l = pd.read_remote_data('p1', 'lineitem')
    c = c[c['c_mktsegment'] == 'BUILDING']['c_custkey']
    o = o.query('o_orderdate < \'1995-3-15\'')
    o = o[['o_orderdate', 'o_shippriority', 'o_orderkey', 'o_custkey']]
    l = l.query('l_shipdate > \'1995, 3, 15\'')
    l['revenue'] = l['l_extendedprice'] * (1 - l['l_discount'])
    l = l[['l_orderkey', 'revenue']]

    t = c.merge(o, left_on='c_custkey', right_on='o_custkey', how='inner')
    t = t.merge(l, left_on='o_orderkey', right_on='l_orderkey', how='inner')
    t = t[['l_orderkey', 'revenue', 'o_orderdate', 'o_shippriority']]
    t = t.groupby(('l_orderkey', 'o_orderdate', 'o_shippriority'), sort=False).agg({'revenue': 'sum'})
    print(t.genSQL)
    return t

def q_04_v1():
    l = pd.read_remote_data('p1', 'lineitem')
    l = l[l['l_commitdate'] < l['l_receiptdate']]
    o = read_file('orders')
    o = o[(o['o_orderdate'] >= datetime.date(1993, 7, 1))
          &(o['o_orderdate'] < datetime.date(1993, 10, 1))]
    o = o[['o_orderpriority', 'o_orderkey']]

    t = o.groupby('o_orderpriority').count()
    t.sort_values('o_orderpriority')
    return t

def q_10_v1():
    c = pd.read_remote_data('p1', 'customer')
    c = c[['c_custkey', 'c_nationkey', 'c_name', 'c_acctbal', 'c_address', 'c_phone', 'c_comment']]
    o = pd.read_remote_data('p1', 'orders')
    o = o[(o['o_orderdate'] >= datetime.date(1993, 10, 1))
         &(o['o_orderdate'] < datetime.date(1994, 1, 1))]
    o = o[['o_orderkey', 'o_custkey']]
    l = pd.read_remote_data('p1', 'lineitem')
    l = l[l['l_returnflag'] == 'R']
    l['revenue'] = l['l_extendedprice'] * (1 - l['l_discount'])
    l = l[['l_orderkey', 'revenue']]
    n = pd.read_remote_data('p1', 'nation')
    n = n[['n_name', 'n_nationkey']]

    t = c.merge(o, left_on='c_custkey', right_on='o_custkey')
    t = t.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    t = t.merge(n, left_on='c_nationkey', right_on='n_nationkey')
    t = t[['c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment', 'revenue']]
    t = t.groupby(('c_custkey', 'c_name', 'c_acctbal', 'c_phone', 'n_name', 'c_address', 'c_comment')).agg({'revenue': 'sum'})
    t.sort_values('revenue', ascending=False)
    print(t.genSQL)
    return t

def q_13_v1():
    c = pd.read_remote_data('p1', 'customer')
    c = c[['c_custkey']]
    o = pd.read_remote_data('p1', 'orders')
    o = o.query('o_comment not like \'^.*special.*requests.*$\'')
    o = o[['o_orderkey', 'o_custkey']]
    t = c.merge(o, left_on='c_custkey', right_on='o_custkey', how='left')
    t = t[['c_custkey', 'o_orderkey']]
    t = t.groupby('c_custkey').count()
    t = t.groupby('o_orderkey').count()
    t.sort_values(['custdist', 'c_count'], ascending=[False, False])
    print(t.genSQL)
    return t

def measure_time(func, *args):
    start = time.time()
    rs = func(*args)
    rs.materialize()
    end = time.time()
    print(rs.data)
    print('Function {} takes time {}'.format(func, end-start))


if __name__ == '__main__':
    connect('127.0.0.1', 'sf01', 'sf01', 5432, 'postgres', 'postgres')
    measure_time(q_10_v1)
    # udf_func()