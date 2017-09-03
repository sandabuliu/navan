#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'tong'


class ResultProxy(object):
    def __init__(self, result):
        self._result = result
        self._data = None

    @property
    def data(self):
        if self._data is None:
            self._data = self._result.fetchall()
        return [list(_) for _ in self._data]

    @property
    def schema(self):
        return [{'name': key, 'type': self.ttype(key)} for key in self._result.keys()]

    @property
    def columns(self):
        return self._result.keys()

    @property
    def json_data(self):
        return [dict(zip(self.columns, _)) for _ in self.data]

    def ttype(self, name):
        try:
            return self._result._metadata._keymap[name][1][0].type.python_type
        except:
            return self._ttype(name)

    def _ttype(self, name):
        ttype = object
        for res in self._data:
            if res.get(name) is None:
                continue
            ttype = type(res.get(name))
            break
        return ttype

    def __str__(self):
        from pandas import DataFrame
        return str(DataFrame(self.data, columns=[c['name'] for c in self.schema]))


class SQLResult(ResultProxy):
    pass


class ODOResult(ResultProxy):
    @property
    def data(self):
        if self._data is None:
            result = self._result.fillna(type(None))
            self._data = [i[1] for i in result.iterrows()]
        return [list(_) for _ in self._data]

    @property
    def schema(self):
        return [{'name': key, 'type': self.ttype(key)} for key in list(self._result)]

    @property
    def columns(self):
        return list(self._result)

    def ttype(self, name):
        from constants import types
        try:
            return types[str(self._result.dtypes[name])]
        except:
            return self._ttype(name)
