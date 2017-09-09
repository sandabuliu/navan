#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import json
import logging
import functools
from util import token

__author__ = 'tong'

logger = logging.getLogger('query')
logger.addHandler(logging.NullHandler())


def split_column(*args):
    if len(args) == 0:
        return None, None
    elif len(args) == 1:
        return None, args[0]
    else:
        return args[0], args[1]


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, 'json'):
            return obj.json()
        else:
            return json.JSONEncoder.default(self, obj)


class Clause(object):
    def json(self):
        pass

    @property
    def table(self):
        return None

    @property
    def sql(self):
        return self.__repr__()


class Text(Clause):
    def __init__(self, text):
        self._clause = text

    def json(self):
        return {'type': 'text', 'text': self._clause}

    def __str__(self):
        return json.dumps(self.json(), cls=JsonEncoder)

    def __repr__(self):
        return self._clause

    @property
    def text(self):
        return self._clause


class Column(Clause):
    def __init__(self, *value):
        self._table, self._value = split_column(*value)
        self._alias = None
        self._distinct = None

    @property
    def table(self):
        return self._table

    def belong(self, value):
        self._table = value

    @property
    def alias(self):
        return self._alias

    @alias.setter
    def alias(self, value):
        self._alias = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def condition(self, op, other):
        return Condition(self, op, other)

    def __gt__(self, other):
        return self.condition('>', other)

    def __ge__(self, other):
        return self.condition('>=', other)

    def __lt__(self, other):
        return self.condition('<', other)

    def __le__(self, other):
        return self.condition('<=', other)

    def __eq__(self, other):
        return self.condition('=', other)

    def __ne__(self, other):
        return self.condition('!=', other)

    def like(self, other):
        return self.condition('like', other)

    def not_like(self, other):
        return self.condition('not like', other)

    def json(self):
        return {'name': self.value, 'alias': self.alias, 'table': self._table,
                'distinct': self._distinct, 'type': 'field'}

    def __str__(self):
        return json.dumps(self.json(), cls=JsonEncoder)

    def __hash__(self):
        return hash(str(self))

    def label(self, value):
        self.alias = value
        return self

    @property
    def is_distinct(self):
        return self._distinct

    def distinct(self, value=True):
        self._distinct = value
        return self

    def __repr__(self):
        distinct = 'DISTINCT ' if self._distinct else ''
        alias = (' AS %s' % self.alias) if self.alias else ''
        table = '%s.' % self._table if self._table else ''
        return '%s%s%s%s' % (distinct, table, self.value, alias)

    @property
    def sql(self):
        return self.__repr__()


class Condition(Clause):
    def __init__(self, obj, op, other):
        self.object = obj
        self.op = op
        self.other = other

    def __and__(self, other):
        if not isinstance(other, Condition):
            raise Exception('Condition should `and` with another Condition')
        return Condition(self, 'and', other)

    def __or__(self, other):
        if not isinstance(other, Condition):
            raise Exception('Condition should `or` with another Condition')
        return Condition(self, 'or', other)

    def json(self):
        return {'object': self.object.json() if hasattr(self.object, 'json') else {'value': self.object, 'type': 'value'},
                'other': self.other.json() if hasattr(self.other, 'json') else {'value': self.other, 'type': 'value'},
                'op': self.op, 'type': 'condition'}

    def __str__(self):
        return json.dumps(self.json(), cls=JsonEncoder)

    def __repr__(self):
        op = self.op
        objs = [self.object, self.other]
        for i, obj in enumerate(objs):
            if isinstance(obj, Clause):
                objs[i] = obj.__repr__()
            elif isinstance(obj, basestring):
                objs[i] = '"%s"' % obj
            elif obj is None:
                objs[i] = 'NULL'
                op = 'is' if op == '=' else ('is not' if op == '!=' else op)
        return '(%s %s %s)' % (objs[0], op.upper(), objs[1])

    @property
    def sql(self):
        return self.__repr__()


class Function(Column):
    def __init__(self, name=None, *args):
        super(Function, self).__init__(name)
        self._args = args

    @property
    def name(self):
        return self._value

    @property
    def args(self):
        return self._args

    def json(self):
        args = [_.json() if hasattr(_, 'json') else _ for _ in self.args]
        return {'name': self.name, 'alias': self.alias, 'args': args, 'type': 'function'}

    @property
    def sql(self):
        return self.__repr__()

    def __repr__(self):
        alias = (' AS %s' % self.alias) if self.alias else ''
        return '%s(%s)%s' % (self.name, ', '.join([_.__repr__() for _ in self.args]), alias)


class OrderBy(Column):
    def __init__(self, name, order=None):
        super(OrderBy, self).__init__(name)
        self._order = order.lower() if order else order
        if self._order not in ('desc', 'asc'):
            raise Exception('order type should be in (`desc`, `asc`)')

    def json(self):
        ret = super(OrderBy, self).json()
        ret['order'] = self._order
        ret['type'] = 'order_by'
        return ret

    @property
    def order(self):
        return self._order or ''

    def __repr__(self):
        order = (' %s' % self._order) if self._order else ''
        return '%s%s' % (super(OrderBy, self).__repr__(), order.upper())

    @property
    def sql(self):
        return self.__repr__()


class Table(Clause):
    def __init__(self, *args):
        self._args = args
        self._name = None
        self._tables = []
        self._whereclauses = []

    def select_from(self, *tables):
        self._tables += tables
        return self

    def where(self, *whereclause):
        self._whereclauses += whereclause
        return self

    @property
    def name(self):
        if self.type == 0:
            if not self._args:
                raise Exception('`table` lack of args')
            return self._args[0]
        if self._name:
            return self._name
        self._name = token()
        return self._name

    @property
    def type(self):
        if self._tables:
            return 1
        return 0

    @property
    def columns(self):
        if self.type != 1:
            raise Exception('Normal table has no `columns`')
        columns = copy.deepcopy(self._args)
        for c in columns:
            if isinstance(c, Column) and not c.table:
                if len(self.tables) > 1:
                    raise Exception('Column `%s` does not bind to any table from %s'
                                    % (c, self.tables))
                c.belong(self.tables[0])
        return columns

    @property
    def tables(self):
        if self.type != 1:
            raise Exception('Normal table has no `tables`')
        return self._tables

    @property
    def whereclauses(self):
        if self.type != 1:
            raise Exception('Normal table has no `whereclauses`')
        return self._whereclauses

    def json(self):
        args = [c.json() if hasattr(c, 'json') else c for c in self._args]
        wheres = [w.json() if hasattr(w, 'json') else w for w in self._whereclauses]
        return {'type': 'table', 'args': args, 'tables': self._tables, 'where': wheres}

    def __repr__(self):
        return "<todo>"

    @property
    def sql(self):
        return self.__repr__()


class Join(Clause):
    def __init__(self, left, right, *clause):
        self._left = left
        self._right = right
        self._clause = list(clause)
        self._is_out = False

    @property
    def is_out(self):
        return self._is_out

    @is_out.setter
    def is_out(self, value):
        self._is_out = value

    @property
    def left(self):
        return self._left

    @property
    def right(self):
        return self._right

    @property
    def on_clause(self):
        return self._clause

    def json(self):
        return {'type': 'join', 'left': self._left, 'right': self._right, 'on': self._clause}

    def on(self, *clause):
        self._clause += clause

    def __repr__(self):
        return '%s INNER JOIN %s ON (%s)' % (self._left, self._right, ' AND '.join(repr(_) for _ in self._clause))

    @property
    def sql(self):
        return self.__repr__()


func = type('Func', (object, ), {'__getattr__': lambda self, x: functools.partial(Function, x)})()
