#!/usr/bin/env python
# -*- coding: utf-8 -*-

from model import DBMeta
from server import BaseHandler
from query.engine import Engine
from query.connector import Connector

__author__ = 'tong'


def params(dbtype, **kwargs):
    dbtype = dbtype.upper()
    if dbtype in ['CSV', 'EXCEL']:
        return {'filelist': kwargs.get('filelist')}
    if dbtype == 'OTHERDB':
        return {'connect_str': kwargs.get('connect_str')}

    kw = {k: v for k, v in kwargs.items() if k in ['host', 'port', 'username', 'password']}
    if dbtype == 'ORACLE':
        kw['oracle_type'] = kwargs['oracle_type']
        kw['oracle_value'] = kwargs['oracle_value']
    return kw


class DataSourceHandler(BaseHandler):
    def post(self):
        args = self.parse_args([
            {'name': 'name', 'required': True, 'location': 'body'},
            {'name': 'type', 'required': True, 'location': 'body'},
            {'name': 'params', 'required': True, 'location': 'body'}
        ])

        args['params'] = params(args['type'], **args['params'])
        db = DBMeta(self.user_id)
        dss = db.datasource(name=args['name']).all()
        if dss:
            self.response(409, message='已存在名称为 %s 的数据源' % args['name'])
            return

        if not args['params'].get('filelist'):
            connector = Connector(args['type'], **args['params'])
            databases = Engine(connector).databases()
            if args['name'] not in databases:
                self.response(412, u'数据库中不存在 %s 库'  % args['name'])
                return

        ds = db.datasource(**args)
        ds.insert()
        db.commit()
        self.response(message='success')

    def delete(self):
        args = self.parse_args([
            {'name': 'id', 'required': True, 'location': 'args'}
        ])

        db = DBMeta(self.user_id)
        db.datasource(id=args['id']).delete()
        db.commit()
        self.response(message='success')

    def put(self):
        args = self.parse_args([
            {'name': 'id', 'required': True, 'location': 'body'},
            {'name': 'name', 'required': True, 'location': 'body'},
            {'name': 'type', 'required': True, 'location': 'body'},
            {'name': 'params', 'required': True, 'location': 'body'}
        ])

        args['params'] = params(args['type'], **args['params'])
        db = DBMeta(self.user_id)
        db.datasource(id=args['id']).update(**args)
        db.commit()
        self.response(message='success')
