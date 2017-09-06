#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'tong'


def parse_where(rules):
    from query.clause import Column
    where = {}
    for rule in rules:
        if not rule.get('field1') or not rule.get('field2'):
            continue
        table1, field1 = rule['field1']
        table2, field2 = rule['field2']
        keys = where.keys()
        for key in keys:
            if table1 in key or table2 in key:
                value = where.pop(key)
                value.append(Column(table1, field1).label('%s.%s' % (table1, field1)) ==
                             Column(table2, field2).label('%s.%s' % (table2, field2)))
                key = tuple(set(key) | {table1, table2})
                where[key] = value
                break
        else:
            where[(table1, table2)] = [Column(table1, field1).label('%s.%s' % (table1, field1)) ==
                                       Column(table2, field2).label('%s.%s' % (table2, field2))]
    return where


def list_to_str(data, sep=' '):
    return sep.join([_ if isinstance(_, basestring) else str(_) for _ in data])


def chart_data(result, xFields, yFields):
    values = {}
    length = len(xFields)
    for i, name in enumerate(yFields):
        values[name] = [data[length + i] for data in result.data]

    return {
        'columns': [data[:length] for data in result.data],
        'data': values
    }


def function(func_name, name):
    from query.clause import Function, Column
    if func_name == 'distinct':
        return Function('count', Column(name).distinct()).json()
    return Function(func_name, Column(name)).label('%s_%s' % (name, func_name)).json()


FILEMETA = ['CSV', 'EXCEL']
DBMETA = ['MYSQL', 'MSSQL', 'ORACLE']
