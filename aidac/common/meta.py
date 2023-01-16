class Histgram:
    def __init__(self, table_name=None, col_name=None, null_frac=0, n_distinct=0, mcv=None, mcf=None, hist=None):
        self.table_name = table_name
        self.col_name = col_name
        self.null_frac = null_frac
        self.n_distinct = n_distinct
        self.mcv = mcv
        self.mcf = mcf
        self.hist = hist


class MetaInfo:
    def __init__(self, cols=None, nrows=0, cwidth=0, cmetas={}):
        if cols is None:
            cols = []

        self.cols = cols
        self.cwidth = cwidth
        self.nrows = nrows
        self.cmetas = cmetas
        self.to_size = cwidth*nrows
