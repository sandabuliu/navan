#!/usr/bin/env python
# -*- coding: utf-8 -*-

import copy
import logging
from functools import wraps
from binder import bind, clause
from clause import Table
from connector import ConnectorBase
from engine import SQLEngine
from util import token


__author__ = 'tong'

logger = logging.getLogger('query')


def sql_list(array):
    return [a.sql if hasattr(a, 'sql') else str(a) for a in array]


def bind_list(query, array):
    return [bind(query, _) for _ in array]


def query_cache(func):
    @wraps(func)
    def _func(self):
        if not self._data.get(func.__name__):
            self._data[func.__name__] = func(self)
        return self._data[func.__name__]
    return _func


class Query(object):
    def __init__(self, table=None, columns=None, where=None, group_by=None, order_by=None, limit=None):
        query = {
            'table': table, 'columns': columns, 'where': where,
            'group_by': group_by, 'order_by': order_by, 'limit': limit
        }
        self._alias = None
        self._query = query
        self._data = {}
        self._bindobj = None

    @property
    def connector(self):
        if isinstance(self._bindobj, Query):
            return self._bindobj.connector
        if isinstance(self._bindobj, ConnectorBase):
            return self._bindobj
        return None

    @property
    def tables(self):
        if isinstance(self._bindobj, Query):
            return self._bindobj.tables
        if isinstance(self._bindobj, ConnectorBase):
            return self._bindobj.tables
        return {}

    @property
    def engine(self):
        if isinstance(self._bindobj, Query):
            return self._bindobj.engine
        if isinstance(self._bindobj, ConnectorBase):
            return self._bindobj.engine
        return None

    @property
    def query(self):
        return copy.deepcopy(self._query)

    @property
    @query_cache
    def database(self):
        return 'database'

    @property
    @query_cache
    def table(self):
        if isinstance(self.query['table'], basestring):
            return Table(self.query['table'])
        if not self.query['table'] and isinstance(self._bindobj, Query):
            return Table(self._bindobj._alias or token())
        return clause(self.query['table'])

    @property
    @query_cache
    def columns(self):
        if not self.query.get('columns'):
            return []
        return [clause(c) for c in self.query['columns']]

    @property
    @query_cache
    def whereclauses(self):
        if not self.query.get('where'):
            return []
        return [clause(w) for w in self.query['where']]

    @property
    @query_cache
    def groupclauses(self):
        if not self.query.get('group_by'):
            return []
        return [clause(g) for g in self.query['group_by']]

    @property
    @query_cache
    def orderclauses(self):
        if not self.query.get('order_by'):
            return []
        return [clause(w) for w in self.query['order_by']]

    @property
    @query_cache
    def limit(self):
        return self.query.get('limit')

    @property
    def binded_table(self):
        if not self.connector:
            return
        if self.table.name in self.tables:
            return self.tables[self.table.name]
        if isinstance(self._bindobj, Query):
            self._bindobj.alias(self.table.name)
            table = self._bindobj._sql_object()
            self.tables[self.table.name] = table
            return table

        if self.table.type == 1:
            bind(self, self.table)
        else:
            self.connector.reflect(only=[self.table.name])
        return self.tables[self.table.name]

    def _sql_string(self):
        select = 'SELECT'
        select_from = 'FROM'
        name = self.table.name
        columns = [c for c in sql_list(self.columns)] or ['*']
        if isinstance(self._bindobj, Query):
            table = self._bindobj._sql_string()
            name = '(%s) AS %s' % (table, self.table.name)
            if not self.columns:
                columns = [c for c in sql_list(self._bindobj.columns)] or ['*']

        sql_str = '%s %s\n%s %s\n' % (select, ', '.join(columns), select_from, name)
        if self.whereclauses:
            sql_str += ('WHERE '+' AND '.join(sql_list(self.whereclauses))+'\n')
        if self.groupclauses:
            sql_str += ('GROUP BY '+', '.join(sql_list(self.groupclauses))+'\n')
        if self.orderclauses:
            sql_str += ('ORDER BY ' + ', '.join(sql_list(self.orderclauses)) + '\n')
        if self.limit:
            sql_str += ('LIMIT %s' % self.limit)
        return sql_str

    def _sql_object(self):
        from sqlalchemy.sql import select
        from sqlalchemy.sql.elements import and_

        table = self.binded_table
        columns = bind_list(self, self.columns) or table.columns
        ret = select(columns)                                 \
            .select_from(table)                               \
            .order_by(*bind_list(self, self.orderclauses))    \
            .where(and_(*bind_list(self, self.whereclauses))) \
            .group_by(*bind_list(self, self.groupclauses))
        if self.limit:
            ret = ret.limit(self.limit)
        if self._alias:
            ret = ret.alias(self._alias)
        return ret

    def _odo_object(self):
        from blaze import by, compute, merge, head

        table = self.binded_table
        columns = bind_list(self, self.columns) or [table[_] for _ in table.fields]
        table = merge(*columns)
        if self.groupclauses:
            groups = bind_list(self, self.groupclauses)
            groups = [table[_.fields[0]] for _ in groups]
            names = [_.fields[0] for _ in groups]
            groups = merge(*groups) if len(groups) > 1 else groups[0]
            table = by(groups, **{c.fields[0]: c for c in columns if c.fields[0] not in names})
        if self.whereclauses:
            wheres = bind_list(self, self.whereclauses)
            table = table[reduce(lambda x, y: x and y, wheres)]
        if self.orderclauses:
            orders = bind_list(self, self.orderclauses)
            for order in orders.reverse():
                table = table.sort(*order)
        if self.limit:
            table = head(table, self.limit)
        return compute(table)

    @property
    def object(self):
        if isinstance(self.engine, SQLEngine):
            return self._sql_object()
        return self._odo_object()

    @property
    def sql(self):
        if isinstance(self.engine, SQLEngine):
            obj = self._sql_object().compile()
            logger.info('SQL:\n%s, PRAMAS: %s' % (obj, obj.params))
            return str(obj)
        obj = self._sql_string()
        logger.info('SQL: %s' % obj)
        return obj

    def bind(self, bind):
        if bind is None:
            return
        if isinstance(bind, ConnectorBase):
            self._bindobj = bind
        elif isinstance(bind, Query):
            self._bindobj = bind
        else:
            raise Exception('can not bind type: `[%s]%s`' % (type(bind), bind))
        return self

    def deepbind(self, bind):
        if isinstance(self._bindobj, Query):
            self._bindobj.deepbind(bind)
        else:
            self.bind(bind)

    def execute(self):
        from proxy import SQLResult, ODOResult
        if not self.connector:
            raise Exception('You should bind a connector first!')

        if type(self.engine) == SQLEngine:
            return SQLResult(self._sql_object().execute())
        return ODOResult(self._odo_object())

    def alias(self, value):
        self._alias = value
        return self

    def _json(self):
        results = []
        if isinstance(self._bindobj, Query):
            results = self._bindobj._json()
        results.insert(0, {
            'name': self._alias,
            'reference': self.table.name,
            'query': self.query
        })
        return results

    def json(self):
        return {'type': 'query', 'query': self._json()}

    def set_limit(self, value):
        if not self.limit or self.limit > value:
            self._query['limit'] = value

    @classmethod
    def load(cls, query, bind=None):
        if query['type'] == 'table':
            return Query(table=query)
        elif query['type'] == 'query':
            queries = query['query']
        else:
            raise Exception('Not a query!')

        items = {_.get('name'): _ for _ in queries}

        item = items[None]
        results = [Query(**item['query'])]
        while item['reference'] in items:
            item = items[item['reference']]
            results.append(Query(**item['query']))
            results[-1].alias(item['name'])
            if not item['reference']:
                break

        for i, q in enumerate(results):
            if i == len(queries) - 1:
                results[-1].bind(bind)
            else:
                q.bind(results[i + 1])
        return results[0]
