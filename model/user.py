#!/usr/bin/env python
# -*- coding: utf-8 -*-

import hashlib
from sqlalchemy import Column
from sqlalchemy.sql import text
from sqlalchemy.sql.schema import DefaultClause
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, TINYINT, TIMESTAMP
from base import Base, MetaBase, undefined

__author__ = 'tong'


class UserModel(Base):
    __tablename__ = 'user'

    id = Column('id', INTEGER, primary_key=True, nullable=False)
    username = Column('username', VARCHAR(length=50), nullable=False)
    password = Column('password', VARCHAR(length=255), nullable=False)
    status = Column('status', TINYINT(display_width=4, unsigned=True), nullable=False, default=0)
    mobile = Column('mobile', VARCHAR(length=20))
    email = Column('email', VARCHAR(length=255))
    name = Column('name', VARCHAR(charset=u'utf8mb4', collation=u'utf8mb4_unicode_ci', length=100))
    avatar = Column('avatar', VARCHAR(length=255))
    gender = Column('gender', TINYINT(display_width=4))
    ctime = Column('ctime', TIMESTAMP(), nullable=False, server_default=DefaultClause(text('CURRENT_TIMESTAMP')))
    utime = Column('utime', TIMESTAMP(), nullable=False, server_default=DefaultClause(text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')))
    is_del = Column('is_del', TINYINT(display_width=4, unsigned=True), nullable=False, server_default=DefaultClause(text('0')))


class User(MetaBase):
    object = UserModel

    def __init__(self, session, *args, **kwargs):
        self.id = None
        self.username = None
        self.password = None
        self.status = None
        self.mobile = None
        self.email = None
        self.name = None
        self.ctime = None
        self.utime = None
        self.avatar = None
        self.gender = None
        self.is_del = None
        super(User, self).__init__(session, *args, **kwargs)

    def insert(self):
        pwd = self.password
        self.password = undefined
        if self.first():
            raise Exception('reduplicate username')
        self.password = hashlib.md5(pwd).hexdigest()
        self.email = self.username
        self.name = self.username.split('@')[0]
        self.avatar = 'https://avatars2.githubusercontent.com/u/15702192'
        ret = super(User, self).insert()
        self.password = pwd
        self.avatar = undefined
        self.email = undefined
        return ret

    def auth(self):
        from utils.cache import cache
        pwd = self.password
        self.password = hashlib.md5(self.password).hexdigest()
        user = cache.get_json('session', self.username)
        if user:
            if user['password'] != self.password:
                raise Exception('user/password error!')
            else:
                user.pop('password')
                return user
        obj = self.single()
        user = {'id': obj.id, 'username': obj.username, 'avatar': obj.avatar,
                'name': obj.name, 'password': self.password}
        cache.set_json('session', self.username, user)
        self.password = pwd
        user.pop('password')
        return user
