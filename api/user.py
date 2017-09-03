#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from model import DBMeta
from server import BaseHandler

__author__ = 'tong'


class LoginHandler(BaseHandler):
    def post(self):
        args = self.parse_args([
            {'name': 'username', 'required': True, 'location': 'body'},
            {'name': 'password', 'required': True, 'location': 'body'}
        ])

        try:
            db = DBMeta()
            user = db.user(**args).auth()
        except Exception, e:
            self.logger.warn(e, exc_info=True)
            self.response(401, 'Auth failed!')
            return

        args['access'] = time.time()
        self.set_cookie('user', args)
        self.response(**user)
