#!/usr/bin/env python
# -*- coding: utf-8 -*-

from user import User
from datasource import Ds
from vtable import VTable
from chart import Chart
from dashboard import Dashboard
from base import DBSession

__author__ = 'tong'


class DBMeta(object):
    def __init__(self, user_id=None):
        self.session = DBSession
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
        try:
            return self.session.commit()
        except Exception:
            self.session.rollback()
            raise

    def __del__(self):
        self.session.commit()
