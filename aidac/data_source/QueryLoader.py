import yaml
import os
from pathlib import Path

TEMPLATE_PATH = 'template.yaml'
cur = Path(__file__, '..').resolve()
__location__ = cur.joinpath(TEMPLATE_PATH)


class QueryLoader:
    def __init__(self, ds_type):
        with open(__location__, 'r') as f:
            self.template = yaml.safe_load(f)
        self.ds_type = ds_type

    def load_query(self, qry_type):
        return self.template[qry_type][self.ds_type]

    def list_tables(self, schema=None):
        """
        list all tables under a schema
        @return:
        """
        return self.load_query('list_tables')

    def table_columns(self, table_name):
        """
        List table (column) metadata
        @param table_name:
        @return:
        """
        return self.load_query('table_columns').format(table_name)

    def create_table(self, table_name, cols_def):
        """
        Create a table with provided table name and column defination
        @param table_name:
        @param cols_def: key value pairs that indicate column name and type
        @return:
        """
        return self.load_query('create_table').format(table_name, cols_def)

    def copy_data(self, table_name, col):
        """
        Copy data from STDIN
        @param table_name: table to copy the data to (must exist forehead)
        @param col: columns to be inserted
        @return:
        """
        return self.load_query('copy_data').format(table_name, col)

    def row_card(self, table):
        return self.load_query('row_num').format(table)

    def column_card(self, table):
        return self.load_query('column_num').format(table)

    def retrieve_table(self, table):
        return self.load_query('retrieve_table').format(table)

    def get_hist(self, table, column):
        return self.load_query('column_hist').format(table, column)

    def create_estimation_func(self):
        return self.load_query('create_estimate_size')

    def drop_estimation_func(self):
        return self.load_query('drop_estimate_size')

    def get_estimation(self, qry):
        return self.load_query('estimate_size').format(qry)