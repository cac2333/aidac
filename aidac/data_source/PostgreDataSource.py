from __future__ import annotations

import datetime

import numpy as np
import pandas

from aidac.common.DataIterator import generator
from aidac.common.column import Column
from aidac.data_source.DataSource import DataSource
from aidac.data_source.QueryLoader import QueryLoader
import psycopg
from psycopg2.extensions import register_adapter, AsIs

from aidac.data_source.ResultSet import ResultSet

# datatime.Datetime

DS = 'postgres'
ql = QueryLoader(DS)

register_adapter(np.int32, AsIs)
register_adapter(np.int64, AsIs)

typeConverter = { np.int8: 'TINYINT', np.int16: 'SMALLINT', np.int32: 'INT', np.int64: 'NUMERIC'
    , np.float32: 'FLOAT', np.float64: 'FLOAT', np.object: 'VARCHAR(100)', np.object_: 'VARCHAR(100)', bytearray: 'BLOB'
    , 'date': 'DATE', 'time': 'TIME', 'timestamp': 'TIMESTAMP', "datetime": datetime.date};

typeConverter_rev = {'integer': np.int32, 'character varying': np.object, 'double precision': np.float64,
                     'numeric': np.float, 'character': np.object,
                     'boolean': bool, 'date': 'date', 'timestamp without time zone': 'timestamp', "datetime": datetime.date
                     }

constant_converter = {'YES': True, 'NO': False}


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

    def ls_tables(self):
        qry = ql.list_tables()
        return self._execute(qry).get_result_ls()

    def import_table(self, table: str, cols: dict, data):
        # todo: allow to specify the columns to be inserted, maybe also create a col object for cols
        # todo: right now data iterate rows, rooms for optimization later
        import time
        start = time.time()
        column_name = ', '.join(list(cols.keys()))
        with self.__cursor.copy(ql.copy_data(table, column_name)) as copy:
            for row in generator(data):
                copy.write_row(row)
        print('loading data time: '+str(start-time.time()))

    def table_columns(self, table: str):
        qry = ql.table_columns(table)
        rs = self._execute(qry)
        # expected return value from pg:
        # returned value: schemaname, tablename, columnname, columntype, columnsize, columnpos, nullable
        cols = [Column(x[2], typeConverter_rev[x[3]], x[1], x[0], constant_converter[x[-1]], x[2], source_table=x[1]) for x in rs.data]
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
            col_def.append(str(cname)+' '+db_type)
        col_def = ', '.join(col_def)

        qry = ql.create_table(table_name, col_def)
        self._execute(qry)
        return col_def

    def retrieve_table(self, table_name):
        qry = ql.retrieve_table(table_name)
        rs = self._execute(qry)
        return rs.get_result_table()

    def get_hist(self, table_name:str, column_name:str):
        qry = ql.get_hist(table_name, column_name)
        rs = self._execute(qry)
        # n_distinct = -1 if all values are distinct, otherwise a negative fraction is used
        # todo: maybe we can optimise this later
        table_name, null_frac, n_distinct, mcv = rs.get_value()
        # need to calculate the actual distinct values
        n_distinct = self.row_count(table_name) * (-n_distinct)
        return table_name, null_frac, n_distinct, mcv

    def get_estimation(self, qry):
        self._execute(ql.drop_estimation_func())
        self._execute(ql.create_estimation_func())
        qry = ql.get_estimation(qry)
        return self._execute(qry).get_value()

    def _execute(self, qry) -> ResultSet | None:
        self.__cursor.execute(qry)
        if self.__cursor.description is not None:
            # as no record returned for insert, update queries
            # todo: change column type here
            rs = ResultSet(self.__cursor.description, self.__cursor.fetchall())
            return rs
        return None

    def _execute(self, qry, *args) -> ResultSet | None:
        self.__cursor.execute(qry, args)
        if self.__cursor.description is not None:
            # as no record returned for insert, update queries
            # todo: change column type here
            rs = ResultSet(self.__cursor.description, self.__cursor.fetchall())
            return rs
        self.__conn.commit()
        return None
