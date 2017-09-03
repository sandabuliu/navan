#!/usr/bin/env python
# -*- coding: utf-8 -*-

from model import DBMeta
from server import BaseHandler

__author__ = 'tong'


class ListHandler(BaseHandler):
    def get(self):
        dbmeta = DBMeta(self.user_id)
        dashboards = dbmeta.dashboard('id', 'name').all()
        result = [{'id': db.id, 'name': db.name} for db in dashboards]
        self.response(**{
            'total': len(result),
            'dashboards': result
        })

    def post(self):
        args = self.parse_args([
            {'name': 'names', 'required': True, 'location': 'body'}
        ])

        dbmeta = DBMeta(self.user_id)
        for name in args['names']:
            dsbd = dbmeta.dashboard(name=name)
            dashboard = dsbd.first()
            if dashboard:
                continue
            dsbd.insert()
        dbmeta.commit()
        self.response(message='success')
