#!/usr/bin/env python
# -*- coding: utf-8 -*-

from model import DBMeta
from server import BaseHandler
from api.util import parse_where, function

from query.query import Query
from query.engine import Engine
from query.util import get_query
from query.connector import Connector
from query.clause import Table, Column, Condition

__author__ = 'tong'


class TableHandler(BaseHandler):
    def post(self):
        args = self.parse_args([
            {'name': 'name', 'required': True, 'location': 'body'},
            {'name': 'ds_id', 'required': True, 'location': 'body'},
            {'name': 'rules', 'required': True, 'location': 'body'},
            {'name': 'type', 'required': True, 'location': 'body', 'default': 'join'},
        ])

        self.meta = DBMeta(self.user_id)

        if args['type'] == 'join':
            table_json = self.join_table()
        else:
            table_json = self.aggr_table()
        if not table_json:
            return

        vtb = self.meta.vtable(ds_id=args['ds_id'], name=args['name'], query=table_json)
        vtb.insert()
        self.meta.commit()
        self.response(message='success')
        del self.meta

    def join_table(self):
        wheres = parse_where(self.args['rules'])

        ds = self.meta.datasource(id=self.args['ds_id']).single()
        engine = Engine(Connector(user_id=self.user_id, type=ds.type, db=ds.name, **ds.params))
        if len(wheres) != 1:
            self.response(422, '需要最终合成一张表')
            return

        tablenames, clause = wheres.items()[0]
        columns = []
        vtables = self.meta.vtable(ds_id=self.args['ds_id']).all()
        vtables = {tb.name: tb.query for tb in vtables}
        for name in tablenames:
            columns += [Column(name, _['name']).label('%s.%s' % (name, _['name']))
                        for _ in self.schema(engine, vtables, name)]
        return Table(*columns).select_from(*tablenames).where(*clause).json()

    def aggr_table(self):
        rule = self.args['rules']
        table = rule['table']
        return Query(table=table, columns=self.columns, where=self.where,
                     group_by=self.group_by, order_by=None, limit=None).json()

    def schema(self, engine, vtables, name):
        if name in engine.tables():
            return engine.schema(name)
        return [{'name': (_.alias or _.value)} for _ in get_query(vtables[name]).columns]

    @property
    def where(self):
        wheres = []
        filters = self.args['rules'].get('filters', [])
        for condition in filters:
            if not condition['name'] or not condition['operator']:
                continue
            name = condition['name']
            operator = condition['operator']
            value = condition['value']
            c = Condition(Column(name), operator, value)
            wheres.append(c.json())
        return wheres

    @property
    def columns(self):
        y_fields = self.args['rules']['y_fields']
        fields = [_['name'] for _ in y_fields if _['name']]
        result = [function(func_name, name) for name, func_name in fields]
        group_by = self.group_by
        if group_by:
            return group_by + result
        if not result:
            return []
        return result

    @property
    def group_by(self):
        x_fields = self.args['rules']['x_fields']
        if not x_fields:
            return None
        return [Column(_).json() for _ in x_fields]
