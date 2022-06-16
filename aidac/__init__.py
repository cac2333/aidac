import aidac.dataframe.frame as frame
# from aidac.dataframe.LocalTable import read_csv
from aidac.dataframe.Scheduler import Scheduler
from aidac.data_source.DataSourceManager import *

__name__ = 'aidac'

def add_data_source(source: str, host: str, user: str, password: str, db: str, job_name: str = None, port: str = None):
    manager.add_data_source(source, host, user, password, db, job_name, port)


def data_sources():
    pass


def tables():
    manager.tables()


def read_csv(path, delimiter=None, header=None, names=None):
    return frame.read_csv(path, delimiter, header, names)


def from_dict(data):
    return frame.from_dict(data)


def read_remote_data(job: str, table_name: str):
    """
    Create a remote table
    If job or table does not exist, an exception will be raised
    @param job: a data source
    @param table_name: remote table name
    @return:
    """
    source = manager.get_data_source(job)
    return frame.create_remote_table(source, table_name)
