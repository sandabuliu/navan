#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import zlib
import base64
from Crypto.Cipher import AES
from sqlalchemy import TypeDecorator, types
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

__author__ = 'tong'


engine = create_engine(os.environ.get('MYSQL_URL'), echo=False)
session_factory = sessionmaker(bind=engine, autocommit=False)
DBSession = scoped_session(session_factory)

metadata = MetaData(engine)
metadata.reflect()

undefined = type('Undefined', (object, ), {})()


class MetaBase(object):
    object = None

    def __init__(self, session, *args, **kwargs):
        self.session = session
        self.keys = [c.name for c in self.object.__table__.columns]
        self.args = args
        self.whereclause = None
        for key in self.keys:
            if key in kwargs:
                setattr(self, key, kwargs[key])
            else:
                setattr(self, key, undefined)

    @property
    def kwargs(self):
        kwargs = {}
        for key in self.keys:
            value = getattr(self, key, undefined)
            if value != undefined:
                kwargs[key] = value
        return kwargs

    def column(self, name):
        columns = self.object.__table__.columns
        return columns[name]

    def filter(self, clause):
        meta = self.__class__(self.session, *self.args, **self.kwargs)
        meta.whereclause = clause
        return meta

    def meta(self, obj):
        kwargs = {}
        if not obj:
            return None
        for key in self.keys:
            kwargs[key] = getattr(obj, key, undefined)
        return self.__class__(self.session, **kwargs)

    def all(self):
        if self.args:
            query = self.session.query(*self.args).select_from(self.object)
        else:
            query = self.session.query(self.object)
        if self.whereclause is not None:
            query = query.filter(self.whereclause)

        result = []
        objs = query.filter_by(is_del=0, **self.kwargs).all()
        for obj in objs:
            result.append(self.meta(obj))
        return result

    def first(self):
        if self.args:
            query = self.session.query(*self.args).select_from(self.object)
        else:
            query = self.session.query(self.object)
        if self.whereclause is not None:
            query = query.filter(self.whereclause)

        obj = query.filter_by(is_del=0, **self.kwargs).first()
        return self.meta(obj)

    def single(self):
        if self.args:
            query = self.session.query(*self.args).select_from(self.object)
        else:
            query = self.session.query(self.object)
        if self.whereclause is not None:
            query = query.filter(self.whereclause)
        obj = query.filter_by(is_del=0, **self.kwargs).one()
        return self.meta(obj)

    def insert(self):
        obj = self.object()
        for key, value in self.kwargs.items():
            setattr(obj, key, value)
        self.session.add(obj)

    def update(self, **kwargs):
        return self.session.query(self.object).filter_by(is_del=0, **self.kwargs).update(kwargs)

    def delete(self):
        return self.update(is_del=1)

    def __repr__(self):
        return str(self.kwargs)


class Json(TypeDecorator):
    impl = types.TEXT
    _null = None
    _type = object

    @property
    def python_type(self):
        return self._type

    def process_bind_param(self, value, dialect):
        return json.dumps(value)

    def process_literal_param(self, value, dialect):
        return value

    def process_result_value(self, value, dialect):
        try:
            value = json.loads(value)
        except (ValueError, TypeError):
            value = self._null
        return value


class List(Json):
    _null = []
    _type = list


class Dict(Json):
    _null = {}
    _type = dict


class AESJson(Json):
    _null = ''
    _type = str

    AES_OBJ = AES.new('sidashujiaonanga')

    def process_bind_param(self, value, dialect):
        value = super(AESJson, self).process_bind_param(value, dialect)
        value += ' ' * (16 - len(value) % 16)
        value = self.AES_OBJ.encrypt(value)
        return base64.b64encode(zlib.compress(value))

    def process_literal_param(self, value, dialect):
        return value

    def process_result_value(self, value, dialect):
        value = zlib.decompress(base64.b64decode(value))
        value = self.AES_OBJ.decrypt(value)
        return super(AESJson, self).process_result_value(value, dialect)


Base = declarative_base()
