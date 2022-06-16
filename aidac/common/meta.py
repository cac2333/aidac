class MetaInfo:
    def __init__(self, cols=None, ncols=0, nrows=0):
        if cols is None:
            cols = []

        self.cols = cols
        if cols:
            self.ncols = len(cols)
        else:
            self.ncols = ncols
        self.nrows = nrows
