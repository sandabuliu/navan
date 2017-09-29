#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
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


class Register(BaseHandler):
    NEED_AUTHED = False

    def post(self):
        args = self.parse_args([
            {'name': 'username', 'required': True, 'location': 'body'},
            {'name': 'password', 'required': True, 'location': 'body'}
        ])

        reg = re.match(r'^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\.[a-zA-Z0-9_-]+)+$', args['username'])
        if not reg:
            self.response(400, '邮箱格式有误')
            return

        try:
            db = DBMeta()
            db.user(**args).insert()
        except Exception, e:
            self.logger.warn(e, exc_info=True)
            self.response(409, '用户已存在')
            return

        args['access'] = time.time()
        self.set_cookie('user', args)
        self.response(message='success')
