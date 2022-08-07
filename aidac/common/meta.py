class MetaInfo:
    def __init__(self, cols=None, ncols=0, nrows=0):
        if cols is None:
            cols = []

        self.cols = cols
        if ncols:
            self.ncols = ncols
        else:
            self.ncols = len(cols)
        self.nrows = nrows
