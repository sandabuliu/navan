#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging
from sqlalchemy.sql import select
from sqlalchemy import create_engine, MetaData
from proxy import SQLResult, ODOResult
from connector import Connector, ConnectorBase

__author__ = 'tong'

logger = logging.getLogger('query')


def Engine(connobj):
    if isinstance(connobj, dict):
        connobj = Connector(**connobj)
    if isinstance(connobj, ConnectorBase):
        dtype = connobj.type
    elif isinstance(connobj, basestring):
        dtype = connobj.split(':', 1)[0]
    else:
        raise Exception('can not get dtype')
    if dtype in ['csv', 'excel']:
        return ODOEngine(connobj)
    else:
        return SQLEngine(connobj)


class SQLEngine(object):
    def __init__(self, connobj):
        self._connobj = connobj
        self._connect_str = self.get_connect_str(connobj)
        self._engine = self.create_engine()
        self._metadata = self.create_meta()
        self._table_names = None
        self._vtables = {}

    def create_engine(self):
        return create_engine(self._connect_str)

    def create_meta(self):
        return MetaData(self._engine)

    @classmethod
    def get_connect_str(cls, connobj):
        if isinstance(connobj, dict):
            connobj = Connector(**connobj)
        if isinstance(connobj, ConnectorBase):
            return connobj.connect_str
        elif isinstance(connobj, basestring):
            return connobj
        raise Exception('Error %s\'connector type: %s(%s)' % (cls.__name__, connobj, type(connobj)))

    def table(self, name):
        if name in self._vtables:
            return self._vtables[name]
        if name in self._metadata.tables:
            return self._metadata.tables[name]
        if name in self.tables():
            self._metadata.reflect(only=[name], views=True)
        if not self._vtables and isinstance(self._connobj, ConnectorBase):
            self.load_virtual()
        return self._vtables.get(name, self._metadata.tables.get(name))

    def load_virtual(self):
        from util import get_query
        for tb_name, vtable in self._connobj.vtables():
            try:
                query = get_query(vtable).bind(self._connobj).object
                self._vtables[tb_name] = query.alias(tb_name)
            except Exception, e:
                logger.error('Load virtual table failed: %s' % e, exc_info=True)

    def databases(self):
        conn = self._engine.connect()
        dbs = self._engine.dialect.get_schema_names(conn)
        conn.close()
        return dbs

    def tables(self):
        conn = self._engine.connect()
        if not self._table_names:
            self._table_names = self._engine.dialect.get_table_names(conn)
            self._table_names += self._engine.dialect.get_view_names(conn)
            self._table_names.sort()
        return list(self._table_names)

    def schema(self, table):
        from constants import types
        if not self._table_names:
            self._table_names = self._engine.table_names()
        if table in self._table_names:
            columns = self._engine.dialect.get_columns(self._engine, table)
            for column in columns:
                column['type'] = column['type'].python_type
            return columns
        else:
            if not self._vtables:
                self.load_virtual()
            return [{'name': key, 'type': types.get(value.type, value.type)}
                    for key, value in self._vtables[table].columns.items()]

    def preview(self, table, rows=100):
        if table not in self._metadata.tables:
            self._metadata.reflect(only=[table])
        table = self._metadata.tables[table]
        return SQLResult(select([table]).limit(rows).execute())


class ODOEngine(SQLEngine):
    def create_engine(self):
        return None

    def create_meta(self):
        return {}

    def table(self, name):
        from blaze import data
        if name not in self._metadata:
            filename = os.path.join(self._connect_str, name)
            if not os.path.exists(filename):
                raise Exception('表 %s 不存在' % name.encode() if isinstance(name, unicode) else name)
            self._metadata[name] = data(filename)
        return self._metadata[name]

    def databases(self):
        path = os.path.dirname(self._connect_str)
        return os.listdir(path)

    def tables(self):
        return os.listdir(self._connect_str)

    def schema(self, table):
        from constants import types
        df = self.table(table)
        return [{'name': name, 'type': types.get(str(value).strip('?'), str)}
                for name, value in df.dshape[1].dict.items()]

    def preview(self, table, rows=100):
        from blaze import head, compute
        df = self.table(table)
        return ODOResult(compute(head(df, rows)))


if __name__ == '__main__':
    # engine = SQLEngine('mysql://root:123456@192.168.1.150:3306/bin_test?charset=utf8')
    engine = ODOEngine('/Users/tong/Desktop/csv/test')
    print engine.databases()
    print engine.tables()
    print engine.schema('faker_view.csv')
    print engine.preview('faker_view.csv', 10)
    print type(engine.preview('faker_view.csv', 10))