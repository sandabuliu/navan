#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from user import User
from datasource import Ds
from vtable import VTable
from chart import Chart
from dashboard import Dashboard
from base import DBSession

__author__ = 'tong'

logger = logging.getLogger('runtime')


class DBMeta(object):
    def __init__(self, user_id=None):
        self.session = DBSession()
        self.user_id = user_id

    def user(self, *args, **kwargs):
        return User(self.session, *args, **kwargs)

    def datasource(self, *args, **kwargs):
        return Ds(self.session, *args, user_id=self.user_id, **kwargs)

    def vtable(self, *args, **kwargs):
        return VTable(self.session, *args, user_id=self.user_id, **kwargs)

    def chart(self, *args, **kwargs):
        return Chart(self.session, *args, user_id=self.user_id, **kwargs)

    def dashboard(self, *args, **kwargs):
        return Dashboard(self.session, *args, user_id=self.user_id, **kwargs)

    def commit(self):
        return self.session.commit()

    def __del__(self):
        try:
            self.commit()
        except Exception, e:
            logger.warn(str(e), exc_info=True)
