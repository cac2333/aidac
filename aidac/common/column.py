class Column:
    def __init__(self, name, dtype, table=None, schema=None, nullable=True, srccol=None, transform=None, source_table=None):
        self.name = name
        self.dtype = dtype
        self.tablename = table
        self.schema = schema
        self.nullable = nullable
        self.srccol = srccol
        self.column_expr = transform
        self.source_table = source_table

    def full_name(self):
        # return self.tablename[0] + '.' +self.name
        return self.name