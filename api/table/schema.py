#!/usr/bin/env python
# -*- coding: utf-8 -*-

from model import DBMeta
from server import BaseHandler

from query.engine import Engine
from query.connector import Connector
from query.binder import clause


__author__ = 'tong'


class SchemaHandler(BaseHandler):
    def get(self):
        args = self.parse_args([
            {'name': 'ds_id', 'required': True, 'location': 'args', 'cast': int},
            {'name': 'tables', 'required': False, 'location': 'args'}
        ])
        tables = args.get('tables')
        db = DBMeta(self.user_id)
        ds = db.datasource(id=args['ds_id']).single()
        connector = Connector(user_id=self.user_id, type=ds.type, db=ds.name, **ds.params)
        engine = Engine(connector)
        result = [{'name': _, 'schema': [_['name'] for _ in engine.schema(_)]} for _ in engine.tables()
                  if not tables or _ in tables]

        dbmeta = DBMeta(self.user_id)
        vtables = dbmeta.vtable(ds_id=args['ds_id']).all()
        for tb in vtables:
            table = clause(tb.query)
            if not tables or tb.name in tables:
                result.append({'name': tb.name, 'schema': [c.alias or c.value for c in table.columns]})
        self.response(**{'total': len(result), 'schema': result})
