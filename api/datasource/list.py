#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import shutil
from model import DBMeta
from server import BaseHandler

from api.util import FILEMETA

__author__ = 'tong'


class ListHandler(BaseHandler):
    def get(self):
        args = self.parse_args([
            {'name': 'pageno', 'required': False, 'location': 'args'},
            {'name': 'pagesize', 'required': False, 'location': 'args'},
            {'name': 'name', 'required': False, 'location': 'args'}
        ])

        dbmeta = DBMeta(self.user_id)

        kwargs = {}
        if args.get('name'):
            kwargs['name'] = args['name']

        datasources = dbmeta.datasource(**kwargs).all()

        result = []
        for ds in datasources:
            item = {'id': ds.id, 'name': ds.name, 'ctime': ds.ctime, 'utime': ds.utime, 'type': ds.type.upper()}
            item.update(ds.params)
            result.append(item)

        self.response(**{"total": len(result), "datasources": result})

    def delete(self):
        args = self.parse_args([
            {'name': 'ids', 'required': True, 'location': 'args', 'cast': json.loads},
        ])

        db = DBMeta(self.user_id)
        for ds_id in args['ids']:
            ds = db.datasource(id=ds_id)
            datasource = ds.single()
            if datasource.type in FILEMETA:
                pass
            ds.delete()
        db.commit()
        self.response(message='success')