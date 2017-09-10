#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from server import BaseHandler
from api.util import parse_where
from model import DBMeta

from query.query import Query
from query.engine import Engine
from query.util import get_query
from query.connector import Connector
from query.clause import Table, Column, Condition, Text

from api.util import function

__author__ = 'tong'


class JoinPreviewHandler(BaseHandler):
    def get(self):
        args = self.parse_args([
            {'name': 'ds_id', 'required': True, 'location': 'args'},
            {'name': 'tables', 'required': True, 'location': 'args', 'cast': json.loads},
            {'name': 'rules', 'required': False, 'location': 'args', 'cast': json.loads}
        ])

        result = []
        rules = args['rules']
        tables = args['tables']
        dbmeta = DBMeta(self.user_id)
        ds = dbmeta.datasource(id=args['ds_id']).single()
        connector = Connector(user_id=self.user_id, type=ds.type, db=ds.name, **ds.params)
        engine = Engine(connector)
        wheres = parse_where(rules)

        vtables = dbmeta.vtable(ds_id=self.args['ds_id']).all()
        vtables = [(tb.name, tb.query) for tb in vtables]
        for tablenames, clause in wheres.items():
            columns = []
            for name in tablenames:
                columns += [Column(name, _['name']).label('%s.%s' % (name,  _['name'])) for _ in engine.schema(name)]
            table_json = Table(*columns).select_from(*tablenames).where(*clause).json()
            q = Query(table=table_json, limit=10)
            q.bind(connector)
            ret = q.execute()
            result.append({'data': ret.json_data, 'sql': q.sql,
                           'columns': ret.columns, 'names': tablenames})

        keys = sum(wheres.keys(), ())

        tables = [(_, dict(vtables).get(_, _)) for _ in tables if _ not in keys]
        for name, tb in tables:
            q = get_query(tb, 10)
            q.bind(connector)
            ret = q.execute()
            result.append({'data': ret.json_data, 'columns': ret.columns, 'names': [name], 'schema': ret.schema})
        self.response(total=len(result), tables=result)


class AggrPreviewHandler(BaseHandler):
    def get(self):
        args = self.parse_args([
            {'name': 'ds_id', 'required': True, 'location': 'args'},
            {'name': 'table', 'required': True, 'location': 'args'},
            {'name': 'filters', 'required': False, 'location': 'args', 'cast': json.loads, 'defalt': '[]'},
            {'name': 'xFields', 'required': False, 'location': 'args', 'cast': json.loads, 'defalt': '[]'},
            {'name': 'yFields', 'required': False, 'location': 'args', 'cast': json.loads, 'defalt': '[]'}
        ])

        self.dbmeta = DBMeta(self.user_id)
        ds = self.dbmeta.datasource(id=args['ds_id']).single()
        connector = Connector(user_id=self.user_id, type=ds.type, db=ds.name, **ds.params)

        query = self.query
        self.logger.info('%s' % query.json())
        query.deepbind(connector)
        result = query.execute()
        self.response(sql=query.sql, data=result.json_data, schema=result.schema, columns=result.columns)
        del self.dbmeta

    @property
    def where(self):
        wheres = []
        for condition in self.args.get('filters', []):
            if not condition['name'] or not condition['operator']:
                continue
            name = condition['name']
            operator = condition['operator']
            if condition.get('value_type', 'value') == 'value':
                value = condition['value']
            else:
                value = Text(condition['value'])
            c = Condition(Column(name), operator, value)
            wheres.append(c.json())
        return wheres

    @property
    def table(self):
        chart = self.dbmeta.vtable(name=self.args['table'], ds_id=self.args['ds_id']).first()
        if chart:
            return chart.query
        return Table(self.args['table']).json()

    @property
    def columns(self):
        fields = [_['name'] for _ in self.args['yFields'] if _['name']]
        result = [function(func_name, name) for name, func_name in fields]
        group_by = self.group_by
        if group_by:
            return group_by + result
        if not result:
            return []
        return result

    @property
    def group_by(self):
        if not self.args['xFields']:
            return None
        return [Column(_).json() for _ in self.args['xFields']]

    @property
    def query(self):
        table = self.table
        if table['type'] == 'table':
            return Query(table=self.table, columns=self.columns, where=self.where, group_by=self.group_by, limit=10)
        else:
            query = Query.load(self.table)
            name = self.args['table']
            query.alias(name)
            return Query(table=name, columns=self.columns, where=self.where,
                         group_by=self.group_by, limit=10).bind(query)
