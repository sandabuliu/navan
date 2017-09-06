#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'tong'


def token():
    from uuid import uuid4
    from random import randint
    tk = str(uuid4()).replace('-', '')
    st = randint(0, len(tk) - 11)
    return 'tb_' + tk[st:st+10]


def get_query(table, limit=None):
    from query import Query
    from clause import Table
    if isinstance(table, basestring):
        return Query(table=table, limit=limit)
    if isinstance(table, Query):
        table.set_limit(limit)
        return table
    if isinstance(table, Table):
        return Query(table=table.json(), limit=limit)
    if table['type'] == 'table':
        return Query(table=table, limit=limit)
    query = Query.load(table)
    query.set_limit(limit)
    return query
