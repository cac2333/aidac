from __future__ import annotations

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
        for x in self.prereqs:
            x.process()
            x.table.clear_lineage()
        sql = self.table.genSQL
        rs = self.table.datasource._execute(sql)
        return rs

    def add_prereq(self, other:Executable):
        self.prereqs.append(other)


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
        dest.datasource.create_table(src.table_name, scols)
        dest.datasource.import_table(src.table_name, src.data)
        # todo: decide if a local stub should be created

    def process(self):
        """
        Need to process all prerequisites and update the lineage
        @return:
        """
        for x in self.prereqs:
            x.process()

        self.transfer(x.table, self.target)

    def add_prereq(self, other: Executable):
        self.prereqs.append(other)