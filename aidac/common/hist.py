class Histgram:
    def __init__(self, table_name=None, null_frac=0, n_distinct=0, mcv=None):
        self.table_name = table_name
        self.null_frac = null_frac
        self.n_distinct = n_distinct
        self.mcv = mcv


