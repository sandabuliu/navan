#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
from server import BaseHandler
from query.constants import FILE_PATH

__author__ = 'tong'


class FileHandler(BaseHandler):
    def post(self):
        args = self.parse_args([
            {'name': 'path', 'required': False, 'location': 'args', 'default': ''},
        ])
        path = os.path.join(FILE_PATH, str(self.user_id), args['path'])
        if not path.startswith(FILE_PATH):
            self.response(400, message='路径填写有误')
            return
        if not os.path.exists(path):
            os.makedirs(path)

        metas = self.request.files.get('file', [])
        for meta in metas:
            filename = meta['filename']
            filename = os.path.join(path, filename)

            with open(filename, 'wb') as up:
                up.write(meta['body'])
        self.response(message='success')

    def delete(self):
        args = self.parse_args([
            {'name': 'filename', 'required': True, 'location': 'args'},
        ])
        path = os.path.join(FILE_PATH, str(self.user_id), 'datasource', args['filename'])
        if not path.startswith(FILE_PATH):
            self.response(400, message='路径填写有误')
            return

        trash = os.path.join(FILE_PATH, str(self.user_id), 'trash')
        if not os.path.exists(trash):
            os.makedirs(trash)
        if not os.path.exists(path):
            self.response(message='success')
            return
        shutil.move(path, trash)
        self.response(message='success')
