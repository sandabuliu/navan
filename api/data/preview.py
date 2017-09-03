#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from server import BaseHandler
from api.util import parse_where
from model import DBMeta

from query.query import Query
from query.engine import Engine
from query.proxy import ResultProxy
from query.connector import Connector
from query.clause import Table, Column

__author__ = 'tong'



class PreviewHandler(BaseHandler):
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
        tables = [_ for _ in tables if _ not in keys]
        for tb in tables:
            q = Query(table=tb, limit=4)
            q.bind(connector)
            ret = q.execute()
            result.append({'data': ret.json_data, 'columns': ret.columns, 'names': [tb]})
        self.response(**{"total": len(result), "tables": result})
