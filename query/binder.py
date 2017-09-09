#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from sqlalchemy.sql.elements import and_, or_
from clause import Column, Function, Condition, OrderBy, Table, Clause, Text
from connector import ODOConnector

__all__ = ('bind', 'clause')
__author__ = 'tong'

logger = logging.getLogger('query')
logger.addHandler(logging.NullHandler())


def bind(query, claus):
    name = claus.__class__.__name__.lower()
    if isinstance(query.connector, ODOConnector):
        bd = getattr(ODOBinder(query), name, None)
    else:
        bd = getattr(SQLBinder(query), name, None)
    if not bd:
        return claus
    return bd(claus)


def clause(data):
    if not isinstance(data, dict):
        return data
    ttype = str(data.get('type')).lower()
    if ttype == 'query':
        from query import Query
        return Query.load(data)
    meth = globals().get(ttype)
    if not meth:
        raise Exception('query unsupported type: %s, query: %s' % (ttype, data))
    return meth(data)


def text(data):
    return Text(data['text'])


def field(data):
    name = data.get('name')
    c = Column(name)
    c.belong(data.get('table'))
    c.alias = data.get('alias')
    c.distinct(data.get('distinct'))
    return c


def function(data):
    args = [clause(v) for v in data['args']]
    f = Function(data['name'], *args)
    f.alias = data.get('alias')
    f.distinct(data.get('distinct'))
    return f


def condition(data):
    obj = clause(data['object'])
    other = clause(data['other'])
    return Condition(obj, data['op'], other)


def table(data):
    args = [clause(c) for c in data['args']]
    where = [clause(w) for w in data['where']]
    return Table(*args).select_from(*data['tables']).where(*where)


def order_by(data):
    return OrderBy(data['name'], data['order'])


def value(data):
    return data['value']


class SQLBinder(object):
    def __init__(self, query):
        self._query = query

    def text(self, claus):
        from sqlalchemy import text
        return text(claus.text)

    def table(self, claus):
        from sqlalchemy.sql import select, and_
        if claus.name in self._query.tables:
            return self._query.tables[claus.name]

        tables = {}
        for tb in claus.tables:
            if tb not in self._query.tables:
                self._query.connector.reflect(only=[tb])
            tables[tb] = self._query.tables[tb]

        dbtype = self._query.connector.type
        binder = TBBinder(dbtype, tables)
        columns = [binder.column(_) for _ in claus.columns]
        where = []
        for whereclause in claus.whereclauses:
            if isinstance(whereclause, Clause):
                whereclause = binder.bind(whereclause)
            where.append(whereclause)
        tbobj = select(columns)
        for tb in tables.values():
            tbobj = tbobj.select_from(tb)
        tbobj = tbobj.where(and_(*where))
        tbobj = tbobj.alias(claus.name)
        self._query.tables[claus.name] = tbobj
        return tbobj

    def column(self, claus):
        if self._query.table.type == 0:
            tb = self.get_table(claus.table)
        else:
            tb = self.table(self._query.table)

        if claus.value not in tb.columns:
            ttype = 'normal-table' if self._query.table.type == 0 else 'multi-table'
            raise Exception('table(type: %s) `%s` have no column `%s`' % (ttype, tb, claus.value))
        column = tb.columns[claus.value]
        if claus.alias:
            column = column.label(claus.alias)
        if claus.is_distinct:
            column = column.distinct()
        return column

    def orderby(self, claus):
        from sqlalchemy.sql import text
        try:
            ret = self.column(claus)
            if claus.order:
                ret = getattr(ret, claus.order)()
            return ret
        except Exception, e:
            logger.warn('order by bind error: %s' % str(e).replace('\n', ' '))
            return text('%s %s' % (claus.value, claus.order.upper()))

    def function(self, claus):
        import functions
        name = self._query.connector.type
        meth = functions.entry(name, claus.name.lower())(
            *[bind(self._query, _) for _ in claus.args]
        )
        if claus.alias:
            meth = meth.label(claus.alias)
        return meth

    def join(self, claus):
        from sqlalchemy.sql import and_
        left = self.get_table(claus.left)
        right = self.get_table(claus.right)
        left.join(right, and_(*claus), claus.is_out)
        return left

    def condition(self, claus):
        op = claus.op.strip().lower()
        return {
            'and': lambda a, b: and_(a, b),
            'or': lambda a, b: or_(a, b),
            '=': lambda a, b: a == b,
            '!=': lambda a, b: a != b,
            '>=': lambda a, b: a >= b,
            '<=': lambda a, b: a <= b,
            '>': lambda a, b: a > b,
            '<': lambda a, b: a < b,
            'like': lambda a, b: a.like(b),
            'null': lambda a, b: a == None,
            'not null': lambda a, b: a != None
        }[op](bind(self._query, claus.object), bind(self._query, claus.other))

    def get_table(self, table_name=None):
        if not table_name:
            table_name = self._query.table.name

        if table_name in self._query.tables:
            return self._query.tables[table_name]
        self._query.connector.reflect(only=[table_name])
        return self._query.tables[table_name]


class ODOBinder(object):
    def __init__(self, query):
        self._query = query

    def table(self, claus):
        from blaze import join
        if claus.name in self._query.tables:
            return self._query.tables[claus.name]

        self._query.connector.reflect(only=claus.tables)
        tables = {tb: self._query.tables[tb] for tb in claus.tables}
        for col in claus.columns:
            tables[col.table] = tables[col.table].relabel(**{col.value: col.alias})

        join_table = None
        joins = None
        wheres = [True] * len(claus.whereclauses)
        wherecount = wheres.count(True)
        while any(wheres):
            for i, where in enumerate(claus.whereclauses):
                if not wheres[i]:
                    continue
                if where.op != '=':
                    raise Exception('unsupported operator: %s' % where.op)
                if not isinstance(where.object, Column) \
                        or not isinstance(where.other, Column):
                    raise Exception('must be `Column` (%s, %s)' % (where.object, where.other))
                if not join_table:
                    tb1 = tables[where.object.table]
                    tb2 = tables[where.other.table]
                    print 1, tb1.fields, tb2.fields, where.object.alias, where.other.alias
                    join_table = join(tb1, tb2, where.object.alias, where.other.alias)
                    joins = {where.object.table, where.other.table}
                    wheres[i] = False
                elif where.object.table in joins and where.other.table not in joins:
                    tb = tables[where.other.table]
                    join_table = join(join_table, tb, where.object.alias, where.other.alias)
                    joins |= {where.other.table}
                    wheres[i] = False
                elif where.object.table not in joins and where.other.table in joins:
                    tb = tables[where.object.table]
                    join_table = join(join_table, tb, where.other.alias, where.object.alias)
                    joins |= {where.object.table}
                    wheres[i] = False
                elif where.object.table in joins and where.other.table in joins:
                    join_table = join_table[join_table[where.object.alias] == join_table[where.other.alias]]
                    wheres[i] = False
                else:
                    continue
            logger.info('To be processed: %s' % wheres)
            if wherecount == wheres.count(True):
                break
            wherecount = wheres.count(True)
        self._query.tables[claus.name] = join_table
        return join_table

    def column(self, claus):
        table_name = claus.table or self._query.table.name
        tb = self.get_table(claus)
        if claus.alias:
            self._query.tables[table_name] = tb.relabel(**{claus.value: claus.alias})
            tb = self._query.tables[table_name]
        column = tb[claus.alias or claus.value]
        if claus.is_distinct:
            column = column.distinct()
        return column

    def orderby(self, claus):
        return claus.value, not (claus.order != 'desc')

    def function(self, claus):
        import functions

        tb = self.get_table(claus)
        meth = functions.entry(tb, claus.name.lower())(
            *[bind(self._query, _) for _ in claus.args]
        )
        if claus.alias:
            meth = meth.label(claus.alias)
        return meth

    def join(self, claus):
        pass

    def condition(self, claus):
        op = claus.op.strip().lower()
        return {
            'and': lambda a, b: and_(a, b),
            'or': lambda a, b: or_(a, b),
            '=': lambda a, b: a == b,
            '!=': lambda a, b: a != b,
            '>=': lambda a, b: a >= b,
            '<=': lambda a, b: a <= b,
            '>': lambda a, b: a > b,
            '<': lambda a, b: a < b,
            'like': lambda a, b: a.like(b),
            'null': lambda a, b: a == None,
            'not null': lambda a, b: a != None
        }[op](bind(self._query, claus.object), bind(self._query, claus.other))

    def get_table(self, claus):
        table_name = claus.table or self._query.table.name
        if self._query.table.type == 0:
            if table_name in self._query.tables:
                return self._query.tables[table_name]
            self._query.connector.reflect(only=[table_name])
            return self._query.tables[table_name]
        else:
            return self.table(self._query.table)


class TBBinder(object):
    def __init__(self, dbtype, tables):
        self._dbtype = dbtype
        self._tables = tables

    def bind(self, claus):
        name = claus.__class__.__name__.lower()
        return getattr(self, name)(claus)

    def column(self, claus):
        column = self._tables[claus.table].columns[claus.value]
        if claus.alias:
            column = column.label(claus.alias)
        if claus.is_distinct:
            column = column.distinct()
        return column

    def condition(self, claus):
        op = claus.op.strip().lower()
        return {
            'and': lambda a, b: and_(a, b),
            'or': lambda a, b: or_(a, b),
            '=': lambda a, b: a == b,
            '!=': lambda a, b: a != b,
            '>=': lambda a, b: a >= b,
            '<=': lambda a, b: a <= b,
            '>': lambda a, b: a > b,
            '<': lambda a, b: a < b,
            'like': lambda a, b: a.like(b),
            'null': lambda a, b: a == None,
            'not null': lambda a, b: a != None
        }[op](self.bind(claus.object), self.bind(claus.other))

    def function(self, claus):
        import functions
        meth = functions.entry(self._dbtype, claus.name.lower())(
            *[self.bind(_) for _ in claus.args]
        )
        if claus.alias:
            meth = meth.label(claus.alias)
        return meth
