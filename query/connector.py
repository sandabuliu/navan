#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

__author__ = 'tong'


def Connector(type, **kwargs):
    type = type.lower()
    for name, cls in globals().items():
        if type in getattr(cls, 'types', []):
            return cls(type=type, **kwargs)
    return ConnectorBase(type=type, **kwargs)


class ConnectorBase(object):
    types = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self._engine = None
        self._tables = {}

    @property
    def type(self):
        return str(self.kwargs.get('type')).lower()

    @property
    def tables(self):
        return self._tables

    @property
    def engine(self):
        from engine import SQLEngine
        if not self._engine:
            self._engine = SQLEngine(self)
        return self._engine

    @property
    def connect_str(self):
        return self.kwargs.get('connect_str')

    def reflect(self, only=None):
        only = only or []
        for name in only:
            self._tables[name] = self.engine.table(name)


class SQLConnector(ConnectorBase):
    types = ['mysql', 'mssql', 'oracle']

    @property
    def connect_str(self):
        usr = self.kwargs.get('username')
        pwd = self.kwargs.get('password')
        host = self.kwargs.get('host')
        port = self.kwargs.get('port')
        db = self.kwargs.get('db')
        return '%s://%s:%s@%s:%s/%s?charset=utf8' % (self.type, usr, pwd, host, port, db)


class ODOConnector(ConnectorBase):
    types = ['csv', 'excel']

    @property
    def engine(self):
        from engine import ODOEngine
        if not self._engine:
            self._engine = ODOEngine(self)
        return self._engine

    @property
    def connect_str(self):
        from constants import FILE_PATH
        user_id = self.kwargs.get('user_id')
        ds_name = self.kwargs.get('db')
        return os.path.join(FILE_PATH, str(user_id), 'datasource', ds_name)
