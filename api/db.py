#!/usr/bin/env python
# -*- coding: utf-8 -*-

from server import BaseHandler

from query.engine import Engine
from query.connector import Connector

__author__ = 'tong'


class DatabaseHandler(BaseHandler):
    def get(self):
        args = self.parse_args([
            {'name': 'type', 'required': True, 'location': 'args'},
            {'name': 'params', 'required': True, 'location': 'args'}
        ])

        engine = Engine(Connector(user_id=self.user_id, type=args['type'], **args['params']))
        dbs = engine.databases()
        self.response(**{'total': len(dbs), 'databases': dbs})
