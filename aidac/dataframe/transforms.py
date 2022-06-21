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

            def _get_proj_col_info(c: dict | str):
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
                    + '(' + self._source_.genSQL + ') ' + self._source_.table_name  # Source table transform SQL.
                    )

        return sql_text

    def multi_source(self) -> bool:
        return False


class SQLJoinTransform(SQLTransform):
    def __init__(self, source1, source2, src1joincols, src2joincols, join, suffix):
        super().__init__(None);

        if (not (join == 'cross') and len(src1joincols) != len(src2joincols)):
            raise AttributeError('src1joincols and src2joincols should have same number columns');

        self._source1_ = weakref.proxy(source1)
        self._source2_ = weakref.proxy(source2)
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
                projcols = sourcecols;
                projcolumns = list();
                for c in projcols.values():
                    if c.name in self._src1joincols_ and c.name in self._src2joincols_:
                        # if the name is common in two columns, we give it a suffix
                        proj_name = c.name + suffix
                    else:
                        proj_name = c.name
                    pc = copy.deepcopy(c);  # Make a copy of the source column specs.
                    pc.name = proj_name  # Reset the mutable fields for projected column.
                    if tableName:
                        pc.tablename = tableName;
                    pc.srccol = [c.name];
                    pc.transform = None;
                    projcolumns.append((proj_name, pc));
                    # projcolumns[projcol] = pc;
                return projcolumns;

            # columns generated by the join transform can contain columns from both the tables.
            self._columns_ = collections.OrderedDict(
                __extractsrccols__(src1cols, self._source1_.table_name, self._lsuffix_) + __extractsrccols__(src2cols,
                                                                                                             self._source2_.table_name,
                                                                                                             self._rsuffix_))
            # self.__columns__ = { **__extractsrccols__(src1cols, self._src1projcols_), **__extractsrccols__(src2cols, self._src2projcols_) };
        return self._columns_

    @property
    def genSQL(self):

        projcoltxt = None;
        for c in self.columns:  # Prepare the list of columns going into the select statement.
            col = self.columns[c]
            projcoltxt = ((projcoltxt + ', ') if projcoltxt else '') + (
                        col.tablename + '.' + col.srccol[0] + ' AS ' + col.name)

        jointxt = None;  # The join condition SQL.
        if (self._jointype_ == 'cross'):
            jointxt = '';
        elif (isinstance(self._src1joincols_, str)):
            jointxt = ' ON ' + self._source1_.table_name + '.' + self._src1joincols_ + ' = ' + self._source2_.table_name + '.' + self._src2joincols_
        else:
            for j in range(len(self._src1joincols_)):
                jointxt = ((jointxt + ' AND ') if (jointxt) else ' ON ') + self._source1_.table_name + '.' + \
                          self._src1joincols_[j] + ' = ' + self._source2_.table_name + '.' + self._src2joincols_[j];

        sqlText = ('SELECT ' + projcoltxt + ' FROM '
                   + '(' + self._source1_.genSQL + ') ' + self._source1_.table_name  # SQL for source table 1
                   + ' ' + JOIN_TYPE[self._jointype_] + ' '
                   + '(' + self._source2_.genSQL + ') ' + self._source2_.table_name  # SQL for source table 2
                   + jointxt
                   )

        return sqlText


class SQLOrderTransform(SQLTransform):

    def __init__(self, source, orderlist):
        super().__init__(source)

        self._order_ = orderlist

    def _gen_column(self, source):
        pass

        #   df.order(by = ...)

        #
        # def execute_source(self):
        #     pass

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    @property
    def genSQL(self):

        srccols = self._source_.columns
        ordered_col = [self._order_] if isinstance(self._order_, str) else self._order_

        sql_text = 'ORDER BY '

        key_res = ''
        sort_order = ''
        print(ordered_col)
        for key_ in range(len(ordered_col)):
            key = ordered_col[key_]
            if key.endswith("#asc"):
                key_res = key[:-4]
                sort_order = 'asc'
            elif key.endswith("#desc"):
                key_res = key[:-5]
                sort_order = 'desc'
            else:
                key_res = key
                sort_order = 'asc'
            sql_text += key_res + ' ' + sort_order + ' '
        return self._source_.genSQL + ' ' + sql_text


class SQLGroupByTransform(SQLTransform):
    def __init__(self, source, projectcols, groupcols=None):
        super().__init__(source)
        self._projectcols_ = projectcols
        self._groupcols_ = groupcols

    def _gen_column(self, source):

        if not self._columns_:

            colcount = 0

            def _get_proj_col_info(c: dict | str):
                print(f"the input c's looking: {c}")

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

            columns = collections.OrderedDict()
            for col in self._projectcols_:
                srccol, projcoln, coltransform = _get_proj_col_info(col)

                sdbtables = []
                srccols = []
                scol = src_cols.get(srccol)
                print(f"scol is {scol}")
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

    def execute_source(self):
        pass

    @property
    def columns(self):
        # print(self._columns_)
        if not self._columns_:
            self._gen_column(self._source_)
        return self._columns_

    @property
    def genSQL(self):
        projcoltxt = None
        for c in self.columns:  # Prepare the list of columns going into the select statement.
            col = self.columns[c]
            print(str(col))
            projcoltxt = ((projcoltxt + ', ') if (projcoltxt) else '') + ((col.transform.genSQL if (
                col.transform) else col.srccol[0]) + ' AS ' + col.name)

        groupcoltxt = None
        if self._groupcols_:
            for g in self._groupcols_:
                groupcoltxt = ((groupcoltxt + ', ') if (groupcoltxt) else '') + g

        sqlText = ('SELECT ' + projcoltxt + ' FROM '
                   + '(' + self._source_.genSQL + ') ' + self._source_.table_name  # Source table transform SQL.
                   + ((' GROUP BY ' + groupcoltxt) if (groupcoltxt) else '')
                   )
        return sqlText


class SQLFillNA(SQLTransform):
    def __init__(self, source, col, val):
        super().__init__(source)
        self._col_to_fill = col  # if col length is 0, fill all columns
        self._target_val_ = val  # if val is None, fill with 0

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    def _gen_column(self, _source_):
        pass

    @property
    def genSQL(self):
        # TODO : find a general solution to support filling all blanks/ filling particular columns
        sqltext = f'SELECT '
        # col_to_fill = self.columns if isinstance(self._col_to_fill, list) and len(self._col_to_fill) == 0
        # elif isinstance()
        if isinstance(self._col_to_fill, list) and len(self._col_to_fill) == 0:
            col_to_fill = []
            for c in self.columns:
                col = self.columns[c]  # type Column
                col_to_fill.append(col.name)

        elif isinstance(self._col_to_fill, str):
            col_to_fill = [self._col_to_fill]
        else:
            col_to_fill = self._col_to_fill

        for col_name in col_to_fill:
            sqltext = sqltext + ' coalesce' + '( ' + col_name + ',' + "'" + str(
                self._target_val_) + "'" + ') ' + ' AS ' + col_name + ","
        ### to be finished
        sqltext = sqltext[:-1]
        sqltext = sqltext + ' FROM ' + '(' + self._source_.genSQL + ') ' + self._source_.table_name

        return sqltext


class SQLDropduplicateTransform(SQLTransform):

    def __init__(self, source):
        super().__init__(source)

    @property
    def columns(self):

        if not self._columns_:
            self._gen_column(self._source_)
        return self._columns_

    @property
    def genSQL(self):

        projcoltxt = None

        for c in self.columns:
            col = self.columns[c]  # generate the column name
            projcoltxt = ((projcoltxt + ', ') if (projcoltxt) else '') + col.name

        sqlText = ('SELECT DISTINCT ' + projcoltxt + ' FROM '
                   + '(' + self._source_.genSQL + ') ' + self._source_.table_name  # Source table transform SQL.
                   )
        return sqlText

    def _gen_column(self, source):
        self._columns_ = self._source_.columns
        print(self._columns_)


class SQLISNA(SQLTransform):
    ####
    def __init__(self, source, cols):
        super().__init__(source)
        self._targetcols_ = cols

class SQLQuery(SQLTransform):
    def __init__(self, source, expr: str):
        super().__init__(source)
        self._query_ = expr

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    @property
    def genSQL(self):

        cur = ''
        count = 0
        for i, char in enumerate(self._query_):
            if count > 0:
                count -= 1
                continue

            if char == '=' and self._query_[i-1] == '=':
                continue

            if char == '!' and self._query_[i+1] == "=":
                count = 1
                cur += '<>'
                continue


            cur += char

        query = self._source_.genSQL + ' WHERE ' + cur

        return query

class SQLApply(SQLTransform):
    def __init__(self, source):
        super().__init__(source)


class SQLDropNA(SQLTransform):
    def __init__(self, source, cols):
        super().__init__(source)
        self._targetcols_ = cols

    def _gen_column(self, source):
        self._columns_ = self._source_.columns

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    @property
    def genSQL(self):
        sql_text = f'DELETE FROM {self._source_.table_name} where '

        target_col = [self._targetcols_] if isinstance(self._targetcols_, str) else self._targetcols_

        if len(target_col) == 1:
            sql_text = sql_text + target_col[0] + ' is NULL;'
            return sql_text
        else:
            if len(target_col) == 0:
                for c in self.columns:
                    colname = self.columns[c]  # colname here is as type Column in aidac/common/column

                    sql_text = sql_text + colname.name + ' is NULL OR '

                sql_text = sql_text[:-4]
                sql_text = sql_text + ";"
                return sql_text

            for colname in self._targetcols_:
                sql_text = sql_text + colname + " is NULL OR "

            sql_text = sql_text[:-4]
            sql_text = sql_text + ";"
            return sql_text


class SQLAPPEND(SQLTransform):
    def __init__(self, source, col_val_dict):
        super().__init__(source)
        self.col_val_dict = col_val_dict

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    @property
    def genSQL(self):
        pass
