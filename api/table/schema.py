#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from model import DBMeta
from server import BaseHandler

from query.engine import Engine
from query.connector import Connector


__author__ = 'tong'


class SchemaHandler(BaseHandler):
    def get(self):
        args = self.parse_args([
            {'name': 'ds_id', 'required': True, 'location': 'args', 'cast': int},
            {'name': 'tables', 'required': False, 'location': 'args', 'cast': json.loads}
        ])
        tables = args.get('tables')
        db = DBMeta(self.user_id)
        ds = db.datasource(id=args['ds_id']).single()
        connector = Connector(user_id=self.user_id, type=ds.type, db=ds.name, **ds.params)
        engine = Engine(connector)

        dbmeta = DBMeta(self.user_id)
        vtables = dbmeta.vtable(ds_id=args['ds_id']).all()
        all_table = engine.tables() + [_.name for _ in vtables]
        result = [{'name': _, 'schema': [_['name'] for _ in engine.schema(_)]} for _ in all_table
                  if not tables or _ in tables]

        self.response(**{'total': len(result), 'schema': result})
