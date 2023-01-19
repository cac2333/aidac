import random
import time

import aidac

config = __import__('table_config')

_PATH = getattr(config, 'local_data_path')

date_columns = getattr(config, 'date_columns')
db_config = getattr(config, 'db_config')
random.seed(122)


def read_file(table, parse_dates=False):
    print(f'reading {table}')
    return aidac.read_csv(_PATH+table+'.csv', parse_dates=parse_dates)


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


class Dist:
    def __init__(self, dbc:DBConfig, name, tables):
        aidac.add_data_source('postgres', dbc.host, dbc.user, dbc.passwd, dbc.db, 'p1', dbc.port)
        self.tables = tables
        self.name = name
        self._assign_tables_()

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

    def table_loc(self):
        locals, remotes = [], []
        for idx, loc in enumerate(self.loc):
            if loc:
                locals.append(self.tables[idx])
            else:
                remotes.append(self.tables[idx])
        s = f'[Dist ({self.name})]\nlocal tables: {locals}\nremote tables: {remotes}'
        return s


def read_auto_gen_queries(path):
    with open(path, 'r') as f:
        return f.readlines()


if __name__ == "__main__":
    table_list = ['nation', 'customer', 'orders', 'lineitem', 'part', 'partsupp', 'region', 'supplier']
    dist = Dist(DBConfig(db_config), 'p1', table_list)
    nation = dist.nation
    customer = dist.customer
    orders = dist.orders
    lineitem = dist.lineitem
    part = dist.part
    partsupp = dist.partsupp
    region = dist.region
    supplier = dist.supplier

    rpath = 'auto_gen_queries/merged_queries_auto_sf0001.txt'
    wpath = 'auto_gen_queries/result.txt'

    times = []
    for cmd in read_auto_gen_queries(rpath)[:50]:
        # append materialize command at the end
        cmd = cmd.rstrip()+'.materialize()'
        start = time.time()
        exec(cmd)
        end = time.time()
        times.append(end-start)

    with open(wpath, 'w') as f:
        f.write('\n'.join(times))

