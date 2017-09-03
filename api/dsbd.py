#!/usr/bin/env python
# -*- coding: utf-8 -*-

from model import DBMeta
from server import BaseHandler

__author__ = 'tong'


class DashboardHandler(BaseHandler):
    def post(self):
        args = self.parse_args([
            {'name': 'name', 'required': True, 'location': 'body'}
        ])

        db = DBMeta(self.user_id)
        ds = db.dashboard(name=args['name'])
        ds.insert()
        db.commit()
        self.response(message='success')

    def delete(self):
        args = self.parse_args([
            {'name': 'name', 'required': True, 'location': 'args'}
        ])

        db = DBMeta(self.user_id)
        ds = db.dashboard(name=args['name'])
        ds.delete()
        db.commit()
        self.response(message='success')
