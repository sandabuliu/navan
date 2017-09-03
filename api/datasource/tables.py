#!/usr/bin/env python
# -*- coding: utf-8 -*-

from model import DBMeta
from server import BaseHandler
from query.engine import Engine
from query.connector import Connector

__author__ = 'tong'


def get_ds(ds_id):
    return [{'username': 'root', 'host': '127.0.0.1', 'port': 3306, 'password': '123456', 'db': 'mytest', 'type': 'MYSQL'},
            {'username': 'root', 'host': '127.0.0.1', 'port': 3306, 'password': '123456', 'db': 'noah', 'type': 'MYSQL'}][int(ds_id)-1]


class TablesHandler(BaseHandler):
    def get(self):
        args = self.parse_args([
            {'name': 'ds_id', 'required': True, 'location': 'args'}
        ])

        dbmeta = DBMeta(self.user_id)
        ds = dbmeta.datasource(id=args['ds_id']).single()
        vtables = dbmeta.vtable('name', ds_id=args['ds_id']).all()
        engine = Engine(Connector(user_id=self.user_id, type=ds.type, db=ds.name, **ds.params))
        result = engine.tables()
        self.response(**{
            "total": len(result),
            "tables": result,
            "vtables": [tb.name for tb in vtables]
        })
