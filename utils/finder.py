#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
from datetime import datetime
from constants import FILE_PATH

__author__ = 'tong'


class Finder(object):
    def __init__(self, user_id):
        self.user_id = user_id
        self.root = os.path.join(FILE_PATH, str(user_id))
        self.trash = os.path.join(self.root, 'trash')

    def datasource(self, name):
        return os.path.join(self.root, 'datasource', name)

    def put(self, filename, context):
        filename = os.path.join(FILE_PATH, str(self.user_id), filename)
        filepath = os.path.dirname(filename)
        if not filename.startswith(FILE_PATH):
            raise Exception('路径填写有误')
        if not os.path.exists(filepath):
            os.makedirs(filepath)
        with open(filename, 'w') as fp:
            fp.write(context)

    def rm(self, path):
        path = os.path.join(FILE_PATH, str(self.user_id), path)
        if not path.startswith(FILE_PATH):
            raise Exception('路径填写有误')
        filename = '%s.%s' % (os.path.basename(path), datetime.now().strftime('%Y%m%d%H%M%S'))
        if not os.path.exists(path):
            return
        if not os.path.exists(self.trash):
            os.makedirs(self.trash)
        shutil.move(path, os.path.join(self.trash, filename))

    def put_table(self, ds_name, name, context):
        self.put(os.path.join(ds_name, name), context)

    def rm_table(self, ds_name, name):
        self.rm(os.path.join('datasource', ds_name, name))

    def rm_datasource(self, ds_name):
        self.rm(os.path.join('datasource', ds_name))
