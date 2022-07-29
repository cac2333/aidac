import datetime
import time

import psycopg

import aidac
import aidac as pd
import pandas.io.sql as psql

_PATH = 'datasets/'


def connect(host, dbname, schema, port, user, pwd):
    aidac.add_data_source('postgres', host, user, pwd, dbname, 'p1', port)


def read_file(table):
    return pd.read_csv(_PATH+table+'.csv')


def my_query_01():
    """
    203.76
    (q_03) get the revenue for orders before 1995-03-15. order join with lineitem
    @param db:
    @return:
    """
    o = read_file('orders')
    l = pd.read_remote_data('p1', 'lineitem')
    t = o.merge(l, left_on='o_orderkey', right_on='l_orderkey')
    t = t.groupby(('l_orderkey', 'o_orderdate', 'o_shippriority'), ('l_orderkey', 'o_orderdate', 'o_shippriority')).sum(min_count=1)
    return t

def q_01_v1(db):
    l = db.get_table('lineitem')
    l = l[l['l_shipdate'] <= datetime.date(1998, 9, 2)]
    disc_price = l['l_extendedprice'] * (1 - l['l_discount'])
    charge = l['disc_price'] * (1 + l['l_tax'])


def q_01_v2(db):
    l = db.lineitem
    l = l[l['l_shipdate'] <= datetime.date(1998, 9, 2)]
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


def measure_time(func, *args):
    start = time.time()
    func(*args)
    end = time.time()
    print('Function {} takes time {}'.format(func, end-start))


if __name__ == '__main__':
    connect('localhost', 'sf01', 'sf01', 6000, 'sf01', 'sf01')
    measure_time(my_query_01)