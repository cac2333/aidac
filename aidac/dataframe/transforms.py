from __future__ import annotations

import collections
import copy
import weakref
from collections.abc import Iterable

from aidac.dataframe import frame
from aidac.common.column import Column

JOIN_TYPE = {
    'inner': 'INNER JOIN',
    'inner join': 'INNER JOIN',
    'outer': 'FULL OUTER JOIN',
    'left': 'LEFT OUTER JOIN',
    'right': 'RIGHT OUTER JOIN',
    'cross': 'CROSS JOIN'
}

class Transform:
    def transform_name(self):
        pass

    @property
    def genSQL(self):
        pass

    def sources(self):
        pass

    def multi_source(self) -> bool: pass


class TableTransform(Transform):
    def __init__(self, tableTransformFunc):
        self.tableTransformFunc = tableTransformFunc

    def applyTransformation(self, data):
        return self.tableTransformFunc(data)


# ------------------------------------------------

# Base class for all SQL Transformations.

def _col_in_source(source: frame.DataFrame, col: Column):
    if source.has_transform():
        scols = source._transform_.columns
    else:
        scols = source.columns
    if col in scols:
        return True
    return False

def infer_col_type(cols: Column):
    pass


class SQLTransform(Transform):
    def __init__(self, source):
        self._columns_ = None
        if isinstance(source, Iterable):
            self._source_ = [weakref.proxy(s) for s in source]
        else:
            self._source_ = weakref.proxy(source) if source else None

    @property
    # The columns that will be produced once this transform is applied on its source.
    def columns(self):
        if (not self._columns_):
            self._columns_ = copy.deepcopy(self._source_.columns)
        return self._columns_

    # The SQL equivalent of applying this transformation in the database.
    @property
    def genSQL(self):
        return self._source_

    def sources(self):
        return self._source_


class SQLProjectionTransform(SQLTransform):
    def __init__(self, source, projcols):
        super().__init__(source)
        self._projcols_ = projcols

    def _gen_column(self, source):
        if not self._columns_:
            colcount = 0

            def _get_proj_col_info(c: dict|str):
                nonlocal colcount
                colcount += 1
                if isinstance(c, dict):  # check if the projected column is given an alias name
                    sc1 = list(c.keys())[0]  # get the source column name / function
                    pc1 = c.get(sc1)  # and the alias name for projection.
                else:
                    sc1 = pc1 = c  # otherwise projected column name / function is the same as the source column.
                # we only consider one possible source column as the renaming is one-one
                # todo: may need to extend this to use F class
                srccol = sc1
                # projected column alias, use the one given, else take it from the expression if it has one, or else generate one.
                projcol = pc1 if (isinstance(pc1, str)) else (
                    sc1.columnExprAlias if (hasattr(sc1, 'columnExprAlias')) else 'col_'.format(colcount))
                # coltransform = sc1 if (isinstance(sc1, F)) else None
                # todo: extend this to use F class
                coltransform = None
                return srccol, projcol, coltransform

            src_cols = source.columns
            # columns = {};
            columns = collections.OrderedDict();
            for col in self._projcols_:
                srccol, projcoln, coltransform = _get_proj_col_info(col)

                sdbtables = []
                srccols = []
                scol = src_cols.get(srccol)
                if not scol:
                    raise AttributeError("Cannot locate column {} from {}".format(srccol, str(source)))
                else:
                    srccols += (scol.name if (isinstance(scol.name, list)) else [scol.name])
                    sdbtables += (scol.tablename if (isinstance(scol.tablename, list)) else [scol.tablename])

                column = Column(projcoln, scol.dtype)
                column.srccol = srccols
                column.tablename = sdbtables
                column.transform = coltransform
                columns[projcoln] = column
            self._columns_ = columns

    @property
    def columns(self):
        if not self._columns_:
            self._gen_column(self._source_)
        return self._columns_

    @property
    def genSQL(self):
        projcoltxt = None
        for c in self.columns:  # Prepare the list of columns going into the select statement.
            col = self.columns[c];
            projcoltxt = ((projcoltxt + ', ') if (projcoltxt) else '') + ((col.transform.columnExpr if (
                col.transform) else col.srccol[0]) + ' AS ' + col.name);

        sql_text = ('SELECT ' + projcoltxt + ' FROM '
                   + '(' + self._source_.genSQL + ') ' + self._source_.table_name # Source table transform SQL.
                   )

        return sql_text

    def multi_source(self) -> bool:
        return False


class SQLJoinTransform(SQLTransform):
    def __init__(self, source1, source2, src1joincols, src2joincols, join, suffix):
        super().__init__(None);

        if(not(join == 'cross') and len(src1joincols) != len(src2joincols)):
            raise AttributeError('src1joincols and src2joincols should have same number columns');

        self._source1_      = weakref.proxy(source1)
        self._source2_      = weakref.proxy(source2)
        self._source_ = (self._source1_, self._source2_)
        self._src1joincols_ = src1joincols
        self._src2joincols_ = src2joincols

        self._jointype_ = join;
        assert len(suffix) >= 1, "At least one suffix need to be specified"
        self._lsuffix_ = suffix[0]
        self._rsuffix_ = suffix[1] if len(suffix) > 1 else ''


    @property
    def columns(self):
        if not self._columns_:
            src1cols = self._source1_.columns;
            src2cols = self._source2_.columns;

            def __extractsrccols__(sourcecols, tableName=None, suffix=''):
                projcols=sourcecols;
                projcolumns = list();
                for c in projcols.values():
                    if c.name in self._src1joincols_ and c.name in self._src2joincols_:
                        # if the name is common in two columns, we give it a suffix
                        proj_name = c.name+suffix
                    else:
                        proj_name = c.name
                    pc = copy.deepcopy(c); #Make a copy of the source column specs.
                    pc.name = proj_name   #Reset the mutable fields for projected column.
                    if tableName:
                        pc.tablename = tableName;
                    pc.srccol = [c.name];
                    pc.transform = None;
                    projcolumns.append((proj_name, pc));
                    #projcolumns[projcol] = pc;
                return projcolumns;

            #columns generated by the join transform can contain columns from both the tables.
            self._columns_ = collections.OrderedDict(__extractsrccols__(src1cols, self._source1_.table_name, self._lsuffix_) +  __extractsrccols__(src2cols, self._source2_.table_name, self._rsuffix_))
            #self.__columns__ = { **__extractsrccols__(src1cols, self._src1projcols_), **__extractsrccols__(src2cols, self._src2projcols_) };
        return self._columns_

    @property
    def genSQL(self):

        projcoltxt=None;
        for c in self.columns: #Prepare the list of columns going into the select statement.
            col = self.columns[c]
            projcoltxt = ((projcoltxt+', ') if projcoltxt else '') + (col.tablename + '.' + col.srccol[0] + ' AS ' + col.name)

        jointxt=None; #The join condition SQL.
        if(self._jointype_ == 'cross'):
            jointxt = '';
        elif(isinstance(self._src1joincols_, str)):
            jointxt = ' ON ' + self._source1_.table_name + '.' + self._src1joincols_ + ' = ' + self._source2_.table_name + '.' + self._src2joincols_
        else:
            for j in range(len(self._src1joincols_)):
                jointxt = ((jointxt + ' AND ') if(jointxt) else ' ON ') + self._source1_.table_name + '.' + self._src1joincols_[j] + ' = ' + self._source2_.table_name + '.' + self._src2joincols_[j];

        sqlText = ( 'SELECT ' + projcoltxt + ' FROM '
                    + '(' + self._source1_.genSQL + ') ' + self._source1_.table_name #SQL for source table 1
                    + ' ' + JOIN_TYPE[self._jointype_] + ' '
                    + '(' + self._source2_.genSQL + ') ' + self._source2_.table_name  #SQL for source table 2
                        + jointxt
                    )

        return sqlText
