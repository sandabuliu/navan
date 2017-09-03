#!/usr/bin/env python
# -*- coding: utf-8 -*-

from model import DBMeta
from server import BaseHandler
from api.util import parse_where

from query.engine import Engine
from query.connector import Connector
from query.clause import Table, Column

__author__ = 'tong'


def get_ds(ds_id):
    return [{'username': 'root', 'host': '127.0.0.1', 'port': 3306, 'password': '123456', 'db': 'mytest', 'type': 'MYSQL'},
            {'username': 'root', 'host': '127.0.0.1', 'port': 3306, 'password': '123456', 'db': 'noah', 'type': 'MYSQL'}][int(ds_id)-1]


class TableHandler(BaseHandler):
    def post(self):
        args = self.parse_args([
            {'name': 'name', 'required': True, 'location': 'body'},
            {'name': 'ds_id', 'required': True, 'location': 'body'},
            {'name': 'rules', 'required': True, 'location': 'body'}
        ])

        rules = args['rules']
        db = DBMeta(self.user_id)
        ds = db.datasource(id=args['ds_id']).single()

        engine = Engine(Connector(user_id=self.user_id, type=ds.type, db=ds.name, **ds.params))
        wheres = parse_where(rules)

        if len(wheres) != 1:
            self.response(422, '需要最终合成一张表')
            return

        tablenames, clause = wheres.items()[0]
        columns = []
        for name in tablenames:
            columns += [Column(name, _['name']).label('%s.%s' % (name,  _['name'])) for _ in engine.schema(name)]
        table_json = Table(*columns).select_from(*tablenames).where(*clause).json()

        vtb = db.vtable(ds_id=args['ds_id'], name=args['name'], query=table_json)
        vtb.insert()
        db.commit()
        self.response(message='success')
