import time

import connectorx as cx
import psycopg

import pandas as pd
import pandas.io.sql as psql

config = __import__('table_config')
db_config = getattr(config, 'db_config')

host = db_config['host']
port = db_config['port']
db = db_config['db']
user = db_config['user']
passwd = db_config['passwd']

p1 = f"postgresql://{user}:{passwd}@{host}:{port}/{db}"
p1_con = psycopg.connect(
            '''host={} 
            dbname={} 
            user={} 
            port={} 
            password={}'''.format(host, db, user, port, passwd)
        )


def load_with_cx(sql, column=None, n=1):
    print(p1)
    rs = cx.read_sql(conn=p1, query=sql, return_type='pandas')
    print(rs)


def load_with_pd(sql):
    t = pd.DataFrame(psql.read_sql_query(sql, p1_con))


def load_manually(sql):
    cursor = p1_con.cursor()
    cursor.execute(sql)
    if cursor.description is not None:
        from aidac.data_source.ResultSet import ResultSet
        rs = ResultSet(cursor.description, cursor.fetchall())
        t = pd.DataFrame(rs.to_tb())


if __name__ == '__main__':
    funcs = [load_with_cx, load_with_pd, load_manually]
    sqls = [
        """SELECT * FROM lineitem""",
        """SELECT SQLQuery4.o_orderkey AS o_orderkey, SQLQuery4.o_custkey AS o_custkey, 
        SQLQuery4.o_orderstatus AS o_orderstatus, SQLQuery4.o_totalprice AS o_totalprice, 
        SQLQuery4.o_orderdate AS o_orderdate, SQLQuery4.o_orderpriority AS o_orderpriority, 
        SQLQuery4.o_clerk AS o_clerk, SQLQuery4.o_shippriority AS o_shippriority, SQLQuery4.o_comment AS o_comment, 
        lineitem.l_orderkey AS l_orderkey, lineitem.l_partkey AS l_partkey, lineitem.l_suppkey AS l_suppkey, 
        lineitem.l_linenumber AS l_linenumber, lineitem.l_quantity AS l_quantity, 
        lineitem.l_extendedprice AS l_extendedprice, lineitem.l_discount AS l_discount, lineitem.l_tax AS l_tax, 
        lineitem.l_returnflag AS l_returnflag, lineitem.l_linestatus AS l_linestatus, lineitem.l_shipdate AS l_shipdate, 
        lineitem.l_commitdate AS l_commitdate, lineitem.l_receiptdate AS l_receiptdate, lineitem.l_shipinstruct AS l_shipinstruct, 
        lineitem.l_shipmode AS l_shipmode, lineitem.l_comment AS l_comment FROM 
        (SELECT * FROM orders WHERE o_orderpriority='1-URGENT') SQLQuery4 
        INNER JOIN (SELECT * FROM lineitem) lineitem ON SQLQuery4.o_orderkey = lineitem.l_orderkey"""
    ]
    for sql in sqls:
        print(f"** Test: {sql}")
        for func in funcs:
            start = time.time()
            func(sql)
            end = time.time() - start
            print(f'{func.__name__} takes {end}')





