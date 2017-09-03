#!/usr/bin/env python
# -*- coding: utf-8 -*-

import functools

__author__ = 'tong'


def entry(name, func_name):
    if isinstance(name, basestring):
        return SQLFunction(name).func_entry(func_name)
    else:
        return ODOFunction(name).func_entry(func_name)


class SQLFunction(object):
    def __init__(self, name):
        from sqlalchemy import dialects
        compiler = dialects.registry.load(name).statement_compiler
        compiler.visit_min_long_func = lambda s, fn, **kw: '0'
        self.name = name

    def func_entry(self, func_name):
        from sqlalchemy.sql import func

        func_entry = {
            'entry': {
                'format_second': lambda x, y, z, a, b, c: func.concat(x, '-', y, '-', z, ' ', a, ':', b, ':', c),
                'format_minute': lambda x, y, z, a, b: func.concat(x, '-', y, '-', z, ' ', a, ':', b, ':00'),
                'format_hour': lambda x, y, z, a: func.concat(x, '-', y, '-', z, ' ', a, ':00:00'),
                'format_day': lambda x, y, z: func.concat(x, '-', y, '-', z),
                'format_month': lambda x, y: func.concat(x, '-', y),
                'format_year': lambda x: func.concat(x)
            },
            'mysql': {},
            'mssql': {}
        }
        et = func_entry.get('entry')
        et.update(func_entry.get(self.name, {}))
        return et.get(func_name, getattr(func, func_name))


class ODOFunction(object):
    def __init__(self, name):
        self.name = name

    def func_entry(self, func_name):
        import blaze
        from blaze.expr import apply

        func_entry = {
            'apply': {},
            'mapper': {
                'avg': 'mean'
            }
        }
        if func_name in func_entry['apply']:
            return lambda *x, **y: apply(self.name, func_entry['apply'][func_name](*x, **y))
        return getattr(blaze, func_entry['mapper'].get(func_name, func_name))
