class DataSource:
    def __init__(self, host, username, password, dbname=None, job_name=None, port=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.dbname = dbname
        self.job_name = job_name
        self.__conn = None
        self.__cursor = None
        self.mapping = {}

    def connect(self):
        pass

    def ls_tables(self):
        """
        List all user tables in current database
        @return: sql query
        """
        pass

    def import_table(self, table_name, cols, data):
        """
        send data to the data source as a table
        @param table_name: insert to table
        @param data: data to be inserted
        @return:
        """
        pass

    def create_table(self, table_name: str, cols: dict):
        """
        create a table using the table name and columns specified
        @param cols:
        @param table_name: table name
        @return:
        """
        pass

    def _execute(self, query: str):
        pass

    def table_columns(self, table: str):
        """
        Retrieve the column metadata of a table
        @param table:
        @return:
        """
        pass

    def row_count(self, table:str):
        """
        Get the row count of a table
        @param table:
        @return: # row
        """
        pass

    def retrieve_table(self, table_name:str):
        pass

    def get_hist(self, table_name:str, column_name:str):
        pass


class LocalDataSource(DataSource):
    def __init__(self):
        self.job_name = '_local_ds'


local_ds = LocalDataSource()


