from __future__ import annotations

import collections
import datetime
import time
from collections import OrderedDict

import numpy as np
import pandas
import logging, sys

from aidac.common.DataIterator import generator
from aidac.common.column import Column
from aidac.data_source.DataSource import DataSource
from aidac.data_source.QueryLoader import QueryLoader
import psycopg
from psycopg import sql

from aidac.data_source.ResultSet import ResultSet

# datatime.Datetime

DS = 'postgres'
ql = QueryLoader(DS)

typeConverter = {np.int8: 'TINYINT', np.int16: 'SMALLINT', np.int32: 'INT', np.int64: 'NUMERIC', float: 'FLOAT'
    , np.float32: 'FLOAT', np.float64: 'FLOAT', np.object: 'VARCHAR(300)', np.object_: 'VARCHAR(300)', bytearray: 'BLOB'
    , datetime.date: 'DATE', datetime.time: 'TIME', 'timestamp': 'TIMESTAMP', np.datetime64: 'TIMESTAMP'}

typeConverter_rev = {'integer': np.int32, 'character varying': np.object, 'double precision': np.float64,
                     'numeric': np.float, 'character': np.object,
                     'boolean': bool, 'date': 'date', 'timestamp without time zone': 'timestamp',
                     "datetime": datetime.date
                     }

constant_converter = {'YES': True, 'NO': False}

def convert_const(key):
    if key in constant_converter:
        return constant_converter[key]
    return key


class PostgreDataSource(DataSource):
    def connect(self):
        self.port = 5432 if self.port is None else self.port

        self.__conn = psycopg.connect(
            """host={} 
            port={} 
            dbname={} 
            user={} 
            password={}""".format(self.host, self.port, self.dbname, self.username, self.password)
        )
        self.__cursor = self.__conn.cursor()
        self._bootstrap()

    def _bootstrap(self):
        # preload all the session temporary functions
        # qry = ql.create_sampled_column_size()
        # qry = ql.create_table_column_meta()
        # self._execute(qry)
        pass

    def ls_tables(self):
        qry = ql.list_tables()
        return self._execute(qry).get_result_ls()

    def import_table(self, table: str, cols: dict, data):
        # todo: right now data iterate rows, rooms for optimization later
        import time
        start = time.time()
        column_name = ', '.join(list(cols.keys()))
        data_len = 0
        width = 0
        # print(f'data to be loaded {data}')
        with self.__cursor.copy(ql.copy_data(table, column_name)) as copy:
            for row in generator(data):
                width = len(row)
                data_len += 1
                copy.write_row(row)
        # print('loading data time: ' + str(start - time.time()))
        # print(f'imported data size {data_len * width}')

    # def table_columns(self, table: str):
    #     qry = ql.table_column_meta(table, 200)
    #     print(qry)
    #     rs = self._execute(qry).data
    #     # expected return value from pg:
    #     # returned value: cname text,
    #     #                  is_nullable bool,
    #     #                  data_type text,
    #     #                 max_val numeric,
    #     #                 avg_len numeric, null_frac numeric, n_distinct numeric
    #     cols = []
    #     for x in rs:
    #         col = Column(name=x[0], table=table,
    #                    dtype=typeConverter_rev[x[2]], nullable=convert_const(x[1]),
    #                    source_table=table, avg_size=float(x[4]), max_val=x[3],
    #                      null_frac=float(x[5]), n_distinct=float(x[6]))
    #         cols.append(col)
    #     return cols

    def table_columns(self, table: str):
        qry = ql.table_columns(table)
        rs = self._execute(qry).data
        # expected return value from pg:
        # returned value: schema, table, col_name,
        #                  is_nullable bool,
        #                  data_type text,
        #                 precision int,
        cols = collections.OrderedDict()
        for x in rs:
            schema, tb_name, col_name, nullable, db_type, precision = x
            col = Column(name=col_name, schema=schema, table=table,
                         dtype=typeConverter_rev[db_type], nullable=convert_const(nullable),
                         source_table=table)
            cols[col_name] = col

        # get the average width for each column
        qry = ql.table_column_width(table)
        rs = self._execute(qry).data
        for name, width in rs:
            cols[name].avg_size = width
        return cols

    def row_count(self, table: str):
        """

        @param table:
        @return:
        """
        qry = ql.row_card(table)
        rows = self._execute(qry).get_value()
        return rows

    def create_table(self, table_name: str, cols: dict):
        """
        create a temporary table inside the db
        @param table_name:
        @param cols: data column definition
        @return: in db column definition
        """
        col_def = []
        for cname, col in cols.items():
            db_type = typeConverter[col.dtype]
            # print(f'converting: {col.dtype} -> {db_type}')
            col_def.append(str(cname) + ' ' + db_type)
        col_def = ', '.join(col_def)

        qry = ql.create_table(table_name, col_def)
        #_print('------------create tb-----------\n{}'.format(qry))
        self._execute(qry)
        return col_def

    def retrieve_table(self, table_name):
        qry = ql.retrieve_table(table_name)
        rs = self._execute(qry)
        return rs.get_result_table()

    def get_hist(self, table_name: str, column_name: str):
        qry = ql.column_stats(table_name, column_name)
        rs = self._execute(qry)
        # n_distinct = -1 if all values are distinct, otherwise a negative fraction is used
        # todo: maybe we can optimise this later
        val = rs.get_value()
        # print(val)
        null_frac, n_distinct, mcv, mcf, hist_bounds, avg_width = val if val else (0, 1, None, None, None, 4)
        # If greater than zero, the estimated number of distinct values in the column.
        # If less than zero, the negative of the number of distinct values divided by the number of rows.
        # todo: maby should store the table meta info within ds
        n_distinct = self.row_count(table_name) * (-n_distinct) if n_distinct < 0 else n_distinct
        return null_frac, n_distinct, mcv, mcf, hist_bounds, avg_width

    def get_estimation(self, qry):
        self._execute(ql.drop_estimation_func())
        self._execute(ql.create_estimation_func())
        qry = ql.get_estimation(qry)
        rs = self._execute(qry)
        return rs.get_value()

    def table_exists(self, table: str):
        qry = ql.table_exists(table)
        r = self._execute(qry).get_value()
        # print(f'table exist = {r}')
        return r

    def _execute(self, qry, *args) -> ResultSet | None:
        start = time.time()
        try:
            self.__cursor.execute(qry, args)
            if self.__cursor.description is not None:
                # as no record returned for insert, update queries
                # todo: change column type here
                rs = ResultSet(self.__cursor.description, self.__cursor.fetchall())
                # print(f'query takes time {time.time() - start}')
                return rs
            self.__conn.commit()
        except psycopg.errors.DatabaseError as err:
            logging.info(f'An error is thrown when executing the following query:\n{qry}')
            logging.info(err)
            logging.info('will rollback shortly...')
            self.__conn.rollback()

        return None


