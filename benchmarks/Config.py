class DBConfig:
    def __init__(self, db_config):
        self.host = db_config['host']
        self.schema = db_config['schema']
        self.db = db_config['db']
        self.port = db_config['port']
        self.user = db_config['user']
        self.passwd = db_config['passwd']