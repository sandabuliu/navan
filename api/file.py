#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from server import BaseHandler
from utils.finder import Finder

__author__ = 'tong'


class FileHandler(BaseHandler):
    def post(self):
        args = self.parse_args([
            {'name': 'path', 'required': False, 'location': 'args', 'default': ''},
        ])
        finder = Finder(self.user_id)
        metas = self.request.files.get('file', [])
        for meta in metas:
            filename = meta['filename']
            filename = os.path.join(args['path'], filename)
            finder.put(filename, meta['body'])
        self.response(message='success')

    def delete(self):
        args = self.parse_args([
            {'name': 'filename', 'required': True, 'location': 'args'},
        ])
        finder = Finder(self.user_id)
        finder.rm(args['path'])
        self.response(message='success')
