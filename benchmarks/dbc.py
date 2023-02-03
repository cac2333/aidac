config = __import__('table_config')

_PATH = getattr(config, 'local_data_path')

date_columns = getattr(config, 'date_columns')
db_config = getattr(config, 'db_config')

import random
import re

import aidac
import pandas as pd


def read_file(table, parse_dates=False):
    print(f'reading {table}')
    return aidac.read_csv(_PATH+table+'.csv', parse_dates=parse_dates)


def read_file_to_pd(table, parse_dates=False):
    return pd.read_csv(''+_PATH+table+'.csv', parse_dates=parse_dates)


def read_tables(local_tbs, remote_tbs):
    all_tbs = {}
    for p in local_tbs:
        if p in date_columns:
            all_tbs[p] = read_file(p, parse_dates=date_columns[p])
        else:
            all_tbs[p] = read_file(p)
    for p in remote_tbs:
        all_tbs[p] = aidac.read_remote_data('p1', p)
    return all_tbs


class DBConfig:
    def __init__(self, db_config):
        self.host = db_config['host']
        self.schema = db_config['schema']
        self.db = db_config['db']
        self.port = db_config['port']
        self.user = db_config['user']
        self.passwd = db_config['passwd']


# class Dist:
#     def __init__(self, job_name, tables):
#         self.tables = tables
#         self.name = job_name
#
#
#
#
#     def table_loc(self):
#         locals, remotes = [], []
#         for idx, loc in enumerate(self.loc):
#             if loc:
#                 locals.append(self.tables[idx])
#             else:
#                 remotes.append(self.tables[idx])
#         s = f'[Dist ({self.name})]\nlocal tables: {locals}\nremote tables: {remotes}'
#         return s
#
#


class Dist:
    def __init__(self, job_name, tables, locals=[], remotes=[], local_only=False):
        self.tables = tables
        self.name = job_name
        self.local_only = local_only
        if locals or remotes:
            self.locals = locals
            self.remotes = remotes
            self.read_tables()
        else:
            self._assign_tables_()

    def read_tables(self):
        if self.local_only:
            for table in self.locals:
                dc = date_columns[table] if table in date_columns else False
                self.__setattr__(table, read_file_to_pd(table, dc))
        else:
            for table in self.locals:
                dc = date_columns[table] if table in date_columns else False
                self.__setattr__(table, read_file(table, dc))
            for table in self.remotes:
                self.__setattr__(table, aidac.read_remote_data(self.name, table))

    def _assign_tables_(self, r=.5):
        # true for local, false for remote
        loc = []
        for _ in self.tables:
            loc.append(random.random()>r)
        self.loc = loc

        for is_local, table in zip(loc, self.tables):
            if is_local:
                dc = date_columns[table] if table in date_columns else False
                self.__setattr__(table, read_file(table, dc))
            else:
                self.__setattr__(table, aidac.read_remote_data(self.name, table))
        self.table_loc()


    def table_loc(self):
        if not self.locals or not self.remotes:
            locals, remotes = [], []
            for idx, loc in enumerate(self.loc):
                if loc:
                    locals.append(self.tables[idx])
                else:
                    remotes.append(self.tables[idx])
            self.locals = locals
            self.remotes = remotes
        s = f'[Dist ({self.name})]\nlocal tables: {self.locals}\nremote tables: {self.remotes}'
        return s


def read_specs(path):
    locals = []
    remotes = []
    with open(path, 'r') as f:
        for line in f.readlines():
            m = re.match(r'local tables: \[(.*)\]', line)
            if m and m[1]:
                locals = m[1].replace("'", "").split(', ')
            m = re.match(r'remote tables: \[(.*)\]', line)
            if m and m[1]:
                remotes = m[1].replace("'", "").split(', ')
    return locals, remotes
