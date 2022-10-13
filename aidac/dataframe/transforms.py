from __future__ import annotations

import collections
import copy
import datetime
import re
import weakref
from collections.abc import Iterable
from enum import Enum
from typing import List

from aidac.common.aidac_types import *
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

    def __str__(self):
        class_str = str(self.__class__)
        match = re.search(r'.*class \'.*\.(.*)\'', class_str)
        if match:
            return match.group(1)
        return ''


class OP(Enum):
    ADD = '+';
    SUBTRACT = '-';
    MULTIPLY = '*';
    DIVIDE = '/';
    NEGATIVE = '(-)';
    EXP = '**';
    MATRIXMULTIPLY = '@';
    TRANSPOSE = 'T';
    LHS = 'LHS';
    RHS = 'RHS';


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


class AlgebraicTransform(Transform):
    def __init__(self, source):
        self._columns_ = None
        self._source_ = source

    @property
    # The columns that will be produced once this transform is applied on its source.
    def columns(self):
        if not self._columns_:
            self._columns_ = copy.deepcopy(self._source_.columns)
        return self._columns_

    @property
    def rows(self):
        return None

    @property
    def genSQL(self):
        return self._source_

    def sources(self):
        return self._source_


class SQLTransform(Transform):
    def __init__(self, source):
        self._columns_ = None
        self._source_ = source

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

    def _construct_col(self, src_cols: dict, srccol: str, projcoln:str, coltransform: str|None, table_name):
        sdbtables = []
        srccols = []
        scol = src_cols.get(srccol)
        if not scol:
            raise AttributeError("Cannot locate column {} from {}".format(srccol, table_name))
        else:
            srccols += (scol.name if (isinstance(scol.name, list)) else [scol.name])
            sdbtables += (scol.tablename if (isinstance(scol.tablename, list)) else [scol.tablename])
            srcol_db = scol.source_table

        column = Column(projcoln, scol.dtype)
        column.srccol = srccols
        column.tablename = sdbtables
        column.column_expr = coltransform
        column.source_table = srcol_db
        return column

class SQLAggregateTransform(SQLTransform):
    def __init__(self, source, projcols, groupcols):
        super().__init__(source)
        self._projcols_ = projcols if isinstance(projcols, Iterable) else [projcols]
        self._groupcols_ = groupcols if isinstance(groupcols, List) else [groupcols]

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
                column = self._construct_col(src_cols, srccol, projcoln, coltransform, str(source))
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
            col = self.columns[c]
            projcoltxt = ((projcoltxt + ', ') if (projcoltxt) else '') + ((col.column_expr if (
                col.column_expr) else col.srccol[0]) + ' AS ' + col.name)

        groupcoltxt = None

        for g in self._groupcols_:
            groupcoltxt = ((groupcoltxt + ", ") if groupcoltxt else "") + g

        if not groupcoltxt:
            sql_text = ('SELECT ' + projcoltxt + ' FROM '
                        + '(' + self._source_.genSQL + ') ' + self._source_.table_name  # Source table transform SQL.
                        )
        else:
            sql_text = ('SELECT ' + projcoltxt + ' FROM '
                        + '(' + self._source_.genSQL + ') ' + self._source_.table_name  # Source table transform SQL.
                        ) + " GROUP BY " + groupcoltxt

        return sql_text


class SQLBinaryOperationTransform(SQLTransform):
    def __init__(self, source, op, other, is_num=False, reverse=False):
        super().__init__(source)
        self.op = op
        self._other_ = other
        self._is_num_ = is_num
        self._reverse_ = reverse

    def _col_exp(self, col):
        col_exp = col.column_expr if col.column_expr else col.full_name()
        if self._is_num_:
            # single column operation
            if self._reverse_:
                exp = '(' + col_exp + self.op + str(self._other_)+')'
            else:
                exp = '(' + str(self._other_) + self.op + col_exp + ')'
            return exp
        else:
            # todo: add multi column operation
            oc = self._other_.columns
            assert(len(oc) == 1)
            ocol_name = next(iter(oc))
            ocol = oc[ocol_name]
            if ocol.column_expr:
                other_text = ocol.column_expr
            else:
                other_text = ocol.full_name()

            # should not worry about the reverse operation
            return '('+other_text + self.op + col_exp+')'

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = super().columns
            for col_name in self._columns_:
                # todo: update column name
                col = self._columns_[col_name]
                # need to assign table name before column expression
                col.column_expr = self._col_exp(col)
        return self._columns_

    @property
    def genSQL(self, partial=False):
        if partial:
            return self._source_.genSQL
        else:
            projcoltxt = None
            for col_name in self.columns:
                col = self.columns[col_name]
                projcoltxt = ((projcoltxt + ', ') if projcoltxt else '') + ((col.column_expr if (
                    col.column_expr) else col.srccol[0]) + ' AS ' + col.name)

            if hasattr(self._source_, 'transform') and isinstance(self._source_.transform, SQLBinaryOperationTransform):
                return 'SELECT ' + projcoltxt + ' FROM (' + self._source_.genSQL(True) + ') ' + self._source_.table_name
            else:
                return 'SELECT ' + projcoltxt + ' FROM (' + self._source_.genSQL + ') ' + self._source_.table_name


class SQLProjectionTransform(SQLTransform):
    def __init__(self, source, projcols):
        super().__init__(source)
        self._projcols_ = projcols

    def _gen_column(self, source):
        if not self._columns_:
            colcount = 0

            def _get_proj_col_info(c: tuple | str):
                nonlocal colcount
                colcount += 1
                if isinstance(c, tuple):  # check if the projected column is given an alias name
                    sc1 = c[0] # get the projected column name / function
                    pc1 = c[1]  # and the actual column
                else:
                    sc1 = pc1 = c  # otherwise projected column name / function is the same as the source column.
                # we only consider one possible source column as the renaming is one-one
                # todo: may need to extend this to use F class
                srccol = sc1
                # projected column alias, use the one given, else take it from the expression if it has one, or else generate one.
                projcol = pc1 if (isinstance(pc1, str)) else sc1
                # coltransform = sc1 if (isinstance(sc1, F)) else None
                # todo: what if the value to be assigned has more than 1 col here?
                coltransform = pc1.column_expr if hasattr(pc1, 'column_expr') else None
                return srccol, projcol, coltransform

            src_cols = source.columns
            # columns = {};
            columns = collections.OrderedDict();

            proj_cols = self._projcols_.items() if hasattr(self._projcols_, 'items') else self._projcols_
            for col in proj_cols:
                srccol, projcoln, coltransform = _get_proj_col_info(col)
                column = self._construct_col(src_cols, srccol, projcoln, coltransform, str(source))
                columns[projcoln] = column
            self._columns_ = columns

    @property
    def columns(self):
        if not self._columns_:
            # the first source will be the main table
            self._gen_column(self._source_[0]) if is_type(self._source_, ArrayLike) else self._gen_column(self._source_)
        return self._columns_

    @property
    def genSQL(self):
        projcoltxt = None
        for c in self.columns:  # Prepare the list of columns going into the select statement.
            col = self.columns[c];
            projcoltxt = ((projcoltxt + ', ') if (projcoltxt) else '') + ((col.column_expr if (
                col.column_expr) else col.srccol[0]) + ' AS ' + col.name);

        # we need to append the selection part here
        # when the operation has the format df[df[]>sth], the projectiontransform holds 2 parents,
        # second one is the condition dataframe and the first is the outer table
        if is_type(self._source_, ArrayLike):
            cond_text = ' '+self._source_[1].transform.gen_condition()
            src_table = self._source_[0]
        else:
            cond_text = ''
            src_table = self._source_

        sql_text = ('SELECT ' + projcoltxt + ' FROM '
                    + '(' + src_table.genSQL + ') ' + src_table.table_name  # Source table transform SQL.
                    + cond_text)

        return sql_text

    def multi_source(self) -> bool:
        return False


class SQLPlaceHolder(SQLTransform):
    def __init__(self, source):
        super().__init__(source)

    @property
    def columns(self):
        if self._columns_ is None:
            self._columns_ = super.columns
        return self._columns_

    @property
    def genSQL(self):
        return self._source_.genSQL


class SQLJoinTransform(SQLTransform):
    def __init__(self, source1, source2, src1joincols, src2joincols, join, suffix):
        super().__init__(None);

        # if(not(join == 'cross') and not isinstance(src1joincols, str) and len(src1joincols) != len(src2joincols)):
        #     raise AttributeError('src1joincols and src2joincols should have same number columns');

        self._source1_ = source1
        self._source2_ = source2
        self._source_ = (self._source1_, self._source2_)
        self._src1joincols_ = src1joincols
        self._src2joincols_ = src2joincols

        self._jointype_ = join
        assert len(suffix) >= 1, "At least one suffix need to be specified"
        self._lsuffix_ = suffix[0]
        self._rsuffix_ = suffix[1] if len(suffix) > 1 else ''

    @property
    def join_cols(self):
        """
        @return: the join columns of 2 sources as a tuple
        """
        if isinstance(self._src1joincols_, str):
            return [(self._src1joincols_, self._src2joincols_)]
        else:
            return [(x, y) for x, y in zip(self._src1joincols_, self._src2joincols_)]

    @property
    def columns(self):
        if not self._columns_:
            src1cols = self._source1_.columns;
            src2cols = self._source2_.columns;

            def __extractsrccols__(sourcecols, tableName=None, suffix=''):
                projcols = sourcecols;
                projcolumns = list();
                for c in projcols.values():
                    if (isinstance(self._src1joincols_,
                                   str) and c.name == self._src1joincols_ and c.name == self._src2joincols_) \
                            or (isinstance(self._src1joincols_,
                                           list) and c.name in self._src1joincols_ and c.name in self._src2joincols_):
                        # if the name is common in two columns, we give it a suffix
                        proj_name = c.name + suffix
                    else:
                        proj_name = c.name
                    pc = copy.deepcopy(c);  # Make a copy of the source column specs.
                    pc.name = proj_name  # Reset the mutable fields for projected column.
                    if tableName:
                        pc.tablename = tableName;
                    pc.srccol = [c.name];
                    pc.column_expr = None;
                    projcolumns.append((proj_name, pc));
                    # projcolumns[projcol] = pc;
                return projcolumns;

            # columns generated by the join transform can contain columns from both the tables.
            self._columns_ = collections.OrderedDict(
                __extractsrccols__(src1cols, self._source1_.table_name, self._lsuffix_) + __extractsrccols__(src2cols, self._source2_.table_name, self._rsuffix_))
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


class SQLRenameTransform(SQLTransform):

    def __init__(self, source, col):
        super().__init__(source)
        self._modifiedcols_ = col

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
            for col_ in self._modifiedcols_.keys():
                col = {col_: self._modifiedcols_.get(col_)}
                srccol, projcoln, coltransform = _get_proj_col_info(col)
                column = self._construct_col(src_cols, srccol, projcoln, coltransform, str(source))
                columns[projcoln] = column
            self._columns_ = columns

    @property
    def columns(self):
        if not self._columns_:
            self._gen_column(self._source_)
        return self._columns_

    @property
    def genSQL(self):
        # print(self._source_.columns)

        sqls = []
        for key in self._modifiedcols_.keys():
            sqltext = 'ALTER TABLE ' + self._source_.table_name + " " + key + ' TO ' + self._modifiedcols_[key] + ';'
            sqls.append(sqltext)
        sqlres = ''.join(sqls)

        return sqlres


class SQLOrderTransform(SQLTransform):

    # order does not reformat the columns
    def __init__(self, source, orderlist, ascending):
        super().__init__(source)

        self._order_ = orderlist
        self._ascending_ = ascending
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
        # print(ordered_col)
        # for key_ in range(len(ordered_col)):
        #     key = ordered_col[key_]
        #     if key.endswith("#asc"):
        #         key_res = key[:-4]
        #         sort_order = 'asc'
        #     elif key.endswith("#desc"):
        #         key_res = key[:-5]
        #         sort_order = 'desc'
        #     else:
        #         key_res = key
        #         sort_order = 'asc'
        #     sql_text += key_res + ' ' + sort_order + ' '
        for key_ in ordered_col:
            sql_text = sql_text + key_ + ' ' + (('asc') if self._ascending_ else 'desc') + ', '
        sql_text = sql_text[:-2]
        return self._source_.genSQL + ' ' + sql_text



class SQLHeadTransform(SQLTransform):
    def __init__(self, source, n):
        super().__init__(source)
        self._num_ = n

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    @property
    def genSQL(self):
        sql_text = self._source_.genSQL + ' LIMIT ' + str(self._num_)
        return sql_text


class SQLTailTransform(SQLTransform):
    def __init__(self, source, n):
        super().__init__(source)
        self._num_ = n

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    @property
    def genSQL(self):
        sql_text = self._source_.genSQL + ' LIMIT ' + str(self._num_) + ' OFFSET' + ' (' + 'SELECT COUNT(*) FROM ' \
                   + self._source_.table_name + ') ' + '- ' + str(self._num_)
        return sql_text


class SQLInsertTransform(SQLTransform):
    def __init__(self, source, column, value):
        super().__init__(source)
        self._insertcols_ = column
        self._values_ = value

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    @property
    def genSQL(self):
        sqltext = 'INSERT INTO ' + self._source_.table_name


class SQLGroupByTransform(SQLTransform):
    def __init__(self, source, groupcols, sort):
        super().__init__(source)
        self._groupcols_ = [groupcols] if isinstance(groupcols, str) else groupcols
        self.sort = sort

    def _gen_column(self, source):


        if not self._columns_:

            colcount = 0

            def _get_proj_col_info(c: dict | str):
                # print(f"the input c's looking: {c}")

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

            # print("src cols:= ",src_cols)
            columns = collections.OrderedDict()

            for col in src_cols:
            # for col in self._groupcols_:
                srccol, projcoln, coltransform = _get_proj_col_info(col)
                column = self._construct_col(src_cols, srccol, projcoln, coltransform, str(source))
                columns[projcoln] = column
            self._columns_ = columns

    def execute_source(self):
        pass

    @property
    def columns(self):
        if not self._columns_:
            self._gen_column(self._source_)
            # self._group_columns_
            # self._columns_ = self._source_.columns
            # print("current self._columns_ is :=",self._columns_)
        return self._columns_

    @property
    def genSQL_compat(self):
        groupcoltxt = None
        if self._groupcols_:
            for g in self._groupcols_:
                groupcoltxt = ((groupcoltxt + ', ') if (groupcoltxt) else '') + g

        sort_coltxt = None
        if self.sort:
            for col in self._groupcols_:
                sort_coltxt = ((sort_coltxt + ", ") if (sort_coltxt) else "") + col

        sql_text = '('+ self._source_.genSQL +')' +self._source_.table_name+ ((' GROUP BY ' + groupcoltxt) if (groupcoltxt) else '' ) + ((" ORDER BY " + sort_coltxt) if self.sort else '')

        return sql_text

    @property
    def genSQL(self):
        projcoltxt = None
        for c in self.columns:  # Prepare the list of columns going into the select statement.
            if c in self._groupcols_:
                col = self.columns[c]

                projcoltxt = ((projcoltxt + ', ') if (projcoltxt) else '') + ((col.column_expr if (
                    col.column_expr) else col.srccol[0]) + ' AS ' + col.name)

        groupcoltxt = None
        if self._groupcols_:
            for g in self._groupcols_:
                groupcoltxt = ((groupcoltxt + ', ') if (groupcoltxt) else '') + g

        sort_coltxt = None
        if self.sort:
            for col in self._groupcols_:
                sort_coltxt = ((sort_coltxt + ", ") if (sort_coltxt) else "") + col

        sqlText = ('SELECT ' + projcoltxt +' FROM '
                   + '(' + self._source_.genSQL + ') ' + self._source_.table_name  # Source table transform SQL.
                   + ((' GROUP BY ' + groupcoltxt) if (groupcoltxt) else '' ) + ((" ORDER BY " + sort_coltxt) if self.sort else '')
                   )
        return sqlText



class SQLFillNA(SQLTransform):
    def __init__(self, source, col, val):
        super().__init__(source)
        self._col_to_fill = col if col else [] # if col length is 0, fill all columns
        self._target_val_ = val if val else 0 # if val is None, fill with 0

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
        sqltext = 'SELECT '
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


class SQLContainsTransform(SQLTransform):
    def __init__(self, source, pat, case, regex):
        super().__init__(source)
        self._pattern_ = pat
        self._case_ = case
        self._regex_ = regex

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    def gen_clauses(self):
        # get the only column
        col = next(iter(self.columns.values()))

        is_all_alnum = True

        if self._regex_:
            return col.name + ' '+'~' + ' ' + "'" + self._pattern_ + "'"

        for i in self._pattern_:
            if not i.isalnum():
                is_all_alnum = False

        if self._regex_ and is_all_alnum:
            self._pattern_ = '%' + self._pattern_ + '%'

        if not self._case_:
            sqltext = 'LOWER' + '(' + col.name + ')' + ' '
            self._pattern_ = self._pattern_.lower()
            if self._regex_:
                sqltext += 'LIKE' + ' ' + "'" + self._pattern_ + "'"
                return sqltext
            else:
                sqltext += 'LIKE' + ' ' + "'" + '%' + self._pattern_ + '%' + "'"
                return sqltext

        else:
            sqltext = col.name + ' '
            if self._regex_:
                sqltext += 'LIKE' + ' ' + "'" + self._pattern_ + "'"
                return sqltext
            else:
                sqltext += 'LIKE' + ' ' + "'" + '%' + self._pattern_ + '%' + "'"
                return sqltext

    def gen_condition(self):
        return ' WHERE ' + self.gen_clauses()

    @property
    def genSQL(self):
        is_all_alnum = True

        for i in self._pattern_:
            if not i.isalnum():
                is_all_alnum = False

        if self._regex_ and is_all_alnum:
            self._pattern_ = '%' + self._pattern_ + '%'

        sqltext = 'SELECT * FROM ' + '(' + self._source_.genSQL + ') ' + self._source_.table_name + self.gen_condition()

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

            if char == '=' and self._query_[i - 1] == '=':
                continue

            if char == '!' and self._query_[i + 1] == "=":
                count = 1
                cur += '<>'
                continue

            cur += char

        query = self._source_.genSQL + ' WHERE ' + cur

        return query


class SQLFilterTransform(SQLTransform):
    def __init__(self, source, op, other, combined=False):
        super().__init__(source)
        self._op_ = self._reformat_op(op)
        self._other_ = other
        self._combined = combined

    def _reformat_op(self, op):
        if op == "eq":
            op_formula= "="
        elif op == "gt":
            op_formula= ">"
        elif op == "ne":
            op_formula= "<>"
        elif op == "ge":
            op_formula= ">="
        elif op == "le":
            op_formula= "<="
        elif op == "lt":
            op_formula= "<"
        else:
            op_formula = op
        return op_formula

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    def is_int(self, o):
        return isinstance(o, int)

    def is_string(self, o):
        return isinstance(o, str)

    def is_float(self, o):
        return isinstance(o, float)

    def is_date_time(self, o):
        return isinstance(o, datetime.date) or isinstance(o, np.datetime64)

    def is_same_type_generic(self, o1, o2):
        """

        @param o1: self column
        @param o2: other values
        @return:
        """
        same_type_flag = True
        if is_type(o2, ArrayLike):
            for o in o2:
                same_type_flag = same_type_flag & self.is_same_type(o, o1)
        else:
            same_type_flag = self.is_same_type(o2, o1)
        return same_type_flag

    def is_same_type(self, o1, o2):
        if self.is_date_time(o1):
            return "date" in str(o2)

        if self.is_int(o1):
            return "int" in str(o2)
        if self.is_float(o1):
            return "float" in str(o2) or "double" in str(o2) or "single" in str(o2)
        if self.is_string(o1):
            return "obj" in str(o2)

    def _format_const(self, val):
        if isinstance(val, str) or self.is_date_time(val):
            return '\'{}\''.format(val)
        elif hasattr(val, 'transform') and hasattr(val.transform, 'gen_clauses'):
            return '('+val.transform.gen_clauses()+')'
        elif is_type(val, ConstantTypes):
            return str(val)
        elif is_type(val, ArrayLike):
            return '('+','.join([self._format_const(v) for v in val])+')'
        else:
            raise ValueError('Filter on unsupported type {}'.format(type(val)))

    def _combine_clauses(self):
        if self._op_ == 'AND' or self._op_ == 'OR':
            return self._format_const(self._source_) + ' {} '.format(self._op_) + self._format_const(self._other_)

    def gen_clauses(self):
        cmb = self._combine_clauses()
        if cmb:
            return cmb

        src_cols = self._source_.columns
        clauses = []
        # compare with a single value
        # todo: does isin support multiple columns?
        if is_type(self._other_, ConstantTypes) or (is_type(self._other_, ArrayLike) and self._op_ == 'in'):
            for cname, col in src_cols.items():
                if self.is_same_type_generic(col.dtype, self._other_):
                    cl = cname + ' ' + self._op_ + ' ' + self._format_const(self._other_)
                    clauses.append(cl)
                else:
                    raise TypeError('Cannot compare {} with {}'.format(col.dtype, type(self._other_)))
        # compare with a list of value
        elif is_type(self._other_, ArrayLike):
            for cname, const_val in zip(src_cols, self._other_):
                col = src_cols[cname]
                if self.is_same_type(const_val, col.dtype):
                    cl = cname + ' ' + self._op_ + ' ' + self._format_const(const_val)
                    clauses.append(cl)
                else:
                    raise TypeError('Cannot compare {} with {}'.format(col.dtype, type(const_val)))
        else:
            # compare with another column
            if len(src_cols) != len(self._other_.columns) or len(self._other_.columns) != 1:
                raise ValueError('The size of compared columns do not match')
            else:
                if len(src_cols) != len(self._other_.columns):
                    other_cols = self._other_.columns.values() * len(src_cols)
                else:
                    other_cols = self._other_.columns.values()

                for mcol, ocol in zip(src_cols.values(), other_cols):
                    if self.is_same_type(mcol.dtype, ocol.dtype):
                        cl = mcol.name + ' ' + self._op_ + ' ' + ocol.name
                        clauses.append(cl)
                    else:
                        raise TypeError('Cannot compare {} with {}'.format(mcol.dtype, ocol.dtype))
        return ' AND '.join(clauses)

    def gen_condition(self):
        where_clause = 'WHERE '+ self.gen_clauses()
        return where_clause

    @property
    def genSQL(self):
        sqltext = "SELECT "

        if self._op_ == "<>":  # ne
            wrong_type_op = '='
        else:
            # all other cases should return false
            wrong_type_op = '<>'

        for c in self.columns:
            col = self.columns[c]
            col_type = col.dtype
            # print(" COLUMN DATETYPE IS : ", col_type)
            col_name = col.name
            if self.is_same_type_generic(col_type, self._other_):
                # print("dddddddd")
                sqltext += "CASE WHEN " + col_name + " " + self._op_ + " " + self._format_const(self._other_) + " THEN TRUE ELSE FALSE END AS " + col_name + ", "
            else:
                sqltext += "CASE WHEN" + " 1 " + wrong_type_op + " 1 THEN TRUE ELSE FALSE END AS " + col_name + ", "

        sqltext = sqltext[:-2]

        sqltext += ' FROM ' + '(' + self._source_.genSQL + ') ' + self._source_.table_name  # Source table transform SQL.

        return sqltext


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

            if char == '=' and self._query_[i - 1] == '=':
                continue

            if char == '!' and self._query_[i + 1] == "=":
                count = 1
                cur += '<>'
                continue

            cur += char

        # replace python str search syntax with db's
        re.sub(r'\.str\.match\(()\)', ' LIKE \\1', cur)
        query = self._source_.genSQL + ' WHERE ' + cur

        return query


# class SQLFilterTransform(SQLTransform):
#     def __init__(self, source, items, like, regex, axis):
#         self.source = source
#         self.items = items
#         self.like = like
#         self.regex = regex
#         self.axis = axis
#
#     @property
#     def columns(self):
#         if not self._columns_:
#             self._columns_ = self._source_.columns
#         return self._columns_
#
#     @property
#     def genSQL(self):

class SQLContainTransform(SQLTransform):
    def __init__(self, source, condition, regex):
        super().__init__(source)
        self._condition_ = condition

        self._regex_ = regex

    @property
    def columns(self):
        # if isinstance(self.collist, dict):
        #     pass
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    @property
    def genSQL(self):

        prev_op = False
        if isinstance(self._source_.transform, SQLProjectionTransform):
            prev_op = True

        sqltext = "SELECT * FROM " + self._source_.table_name + " WHERE "

        if self._regex_:
            sqltext += " LIKE " + "'" + self._condition_ + "'"




class SQLApply(SQLTransform):
    def __init__(self, source, func, axis):
        super().__init__(source)
        self._func_ = func
        self._axis_ = axis


class SQLAGG_Transform(SQLTransform):

    def __init__(self, source, func, collist):
        super().__init__(source)

        self._table_name_ = None
        self.func = func
        self._is_dict_ = isinstance(collist, dict)
        self.collist = collist if len(collist) != 0 else self._source_.columns

    @property
    def columns(self):
        # if isinstance(self.collist, dict):
        #     pass
        if not self._columns_:
            self._gen_column(self._source_)
        return self._columns_

    @property
    def table_name(self):
        if not self._table_name_:
            self._table_name_ = self._source_.table_name
        return self._table_name_

    def _add_groupby_cols(self, source, columns, colcount):
        if source.transform is not None and isinstance(source.transform, SQLGroupByTransform):
            src_cols = source.columns
            for by in source.transform._groupcols_:
                srccol, projcoln, coltransform = by, by, None
                column = self._construct_col(src_cols, srccol, projcoln, coltransform, str(source))
                columns[projcoln] = column
                colcount += 1
            return columns, colcount

    def _gen_column(self, source):

        if not self._columns_:
            colcount = 0

            def _get_new_col(c, operation):
                nonlocal colcount
                colcount += 1
                sc1 = c
                pc1 = c
                srccol = sc1
                projcol = pc1 if (isinstance(pc1, str)) else (
                    sc1.columnExprAlias if (hasattr(sc1, 'columnExprAlias')) else 'col_'.format(colcount))
                coltransform = None
                return srccol, projcol, coltransform

            def _get_proj_col_info(c: dict | str):
                # print(f"the input c's looking: {c}")

                nonlocal colcount
                colcount += 1
                if isinstance(c, dict):  # check if the projected column is given an alias name
                    sc1 = list(c.keys())[0]  # get the source column name / function
                    pc1 = c.get(sc1)  # and the alias name for projection.
                else:
                    sc1 = c  # otherwise projected column name / function is the same as the source column.
                # we only consider one possible source column as the renaming is one-one
                    # keep the original name to be consistent with pandas
                    pc1 = c

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
            # print(isinstance(self.collist, dict))
            if self._is_dict_:
                for col in self.collist:
                    # get all the operations for one column
                    operations = [self.collist[col]] if isinstance(self.collist[col], str) else self.collist[col]

                    for operation in operations:
                        srccol, projcoln, coltransform = _get_new_col(col, operation)
                        if len(operations) > 1:
                            projcoln = operation + '_' + projcoln
                        column = self._construct_col(src_cols, srccol, projcoln, coltransform, str(source))
                        column.agg_func = operation
                        columns[projcoln] = column
            else:
                for col in self.collist:
                    srccol, projcoln, coltransform = _get_proj_col_info(col)
                    column = self._construct_col(src_cols, srccol, projcoln, coltransform, str(source))
                    column.agg_func = self.func
                    columns[projcoln] = column

            columns, colcount = self._add_groupby_cols(source, columns, colcount)
            self._columns_ = columns

    @property
    def genSQL(self):

        has_groupby = False

        if isinstance(self._source_.transform, SQLGroupByTransform):
            has_groupby = True

        # print("check if has groupby previously",has_groupby)

        tb_name = self._source_.table_name
        targetcol = None
        if len(self.collist) == 0:
            targetcol = "(*)"
        projcoltxt = None

        # affected_cols = self._source_.
        if not self._is_dict_:
            for c in self.columns:

                col = self.columns[c]
                projcoltxt = (( projcoltxt + ", ") if (projcoltxt) else '') + (( self.func +'('+ col.column_expr + ')' if (col.column_expr)
                    else self.func +'('+col.srccol[0] + ')') + " AS " + col.name)
        else:
            for c in self.columns:
                col = self.columns[c]
                if col.srccol[0] in self.collist and col.agg_func is not None:
                    projcoltxt = ((projcoltxt + ", ") if (projcoltxt) else '') + ((col.agg_func + '(' + col.column_expr + ')' if (col.column_expr)
                    else col.agg_func + '('+col.srccol[0] + ')') + " AS " + col.name)
                else:
                    # these are groupby columns
                    projcoltxt = ((projcoltxt + ", ") if (projcoltxt) else '') \
                                 + (col.column_expr if col.column_expr else col.srccol[0]) + " AS " + col.name

        if has_groupby:

            sqlText = ("SELECT " + projcoltxt + " FROM "
                        + self._source_.genSQL_compat )
        else:
            sqlText = ("SELECT " + projcoltxt + " FROM "
                       + "(" + self._source_.genSQL+ ") " + self._source_.table_name)

        return sqlText


class SQLDropNA(SQLTransform):
    def __init__(self, source, cols):
        super().__init__(source)
        self._targetcols_ = cols if cols else []

    def _gen_column(self, source):
        self._columns_ = self._source_.columns

    @property
    def columns(self):
        if not self._columns_:
            self._columns_ = self._source_.columns
        return self._columns_

    @property
    def genSQL(self):
        sql_text = 'DELETE FROM {} where '.format(self._source_.table_name)

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
