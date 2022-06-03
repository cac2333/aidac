from __future__ import annotations

import pandas as pd

from aidac.common.DataIterator import generator
from aidac.dataframe import frame


class Executable:
    def __init__(self, df):
        self.df = df
        self.prereqs = []

    def process(self):
        """
        Need to process all prerequisites and update the lineage
        @return:
        """
        if isinstance(self.df, frame.LocalTable):
            return self.df.data

        for x in self.prereqs:
            x.process()
            x.clear_lineage()
        sql = self.df.genSQL
        rs = self.df.source._execute(sql)

        data = rs.get_result_table()
        return pd.DataFrame(data)

    def add_prereq(self, other:Executable):
        self.prereqs.append(other)

    def clear_lineage(self):
        self.df.clear_lineage()


class TransferExecutable(Executable):
    def __init__(self, target, prereqs=[]):
        self.prereqs = prereqs
        self.target = target

    def transfer(self, src: frame.DataFrame, dest: frame.DataFrame):
        """
        Transfer data from one datasource to another
        @param src: source table to be transferred
        @param dest: dataframe whose datasource would be the destination
        @return: local stub points to the temporary table?
        """
        scols = src.columns
        # todo: check for duplicate names
        dest.source.create_table(src.table_name, scols)
        dest.source.import_table(src.table_name, scols, generator(src.data))
        # todo: decide if a local stub should be created

    def process(self):
        """
        Need to process all prerequisites and update the lineage
        @return:
        """
        for x in self.prereqs:
            x.process()

        self.transfer(x.df, self.target)

    def add_prereq(self, other: Executable):
        self.prereqs.append(other)

    def clear_lineage(self):
        return