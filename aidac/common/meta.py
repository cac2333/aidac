class Histgram:
    def __init__(self, table_name=None, null_frac=0, n_distinct=0, mcv=None, col_name=None):
        self.table_name = table_name
        self.col_name = col_name
        self.null_frac = null_frac
        self.n_distinct = n_distinct
        self.mcv = mcv


class MetaInfo:
    def __init__(self, cols=None, cwidth=0, nrows=0):
        if cols is None:
            cols = []

        self.cols = cols
        self.cwidth = cwidth
        self.nrows = nrows
        self.null = 0
        self.cmetas = {}
