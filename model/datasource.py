#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from sqlalchemy import Column
from sqlalchemy.sql import text
from sqlalchemy.sql.schema import DefaultClause
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, TINYINT, TIMESTAMP

from utils.finder import Finder
from model.base import Base, MetaBase, AESJson

__author__ = 'tong'


class DsModel(Base):
    __tablename__ = 'datasource'

    id = Column('id', INTEGER(display_width=11, unsigned=True), primary_key=True, nullable=False)
    name = Column('name', VARCHAR(length=255), nullable=False)
    type = Column('type', VARCHAR(length=64), nullable=False)
    params = Column('params', AESJson())
    user_id = Column('user_id', INTEGER(display_width=11), nullable=False)
    ctime = Column('ctime', TIMESTAMP(), nullable=False, server_default=DefaultClause(text('CURRENT_TIMESTAMP')))
    utime = Column('utime', TIMESTAMP(), nullable=False, server_default=DefaultClause(text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')))
    is_del = Column('is_del', TINYINT(display_width=4, unsigned=True), nullable=False, server_default=DefaultClause(text('0')))


class Ds(MetaBase):
    object = DsModel

    def __init__(self, session, *args, **kwargs):
        self.id = None
        self.name = None
        self.type = None
        self.params = None
        self.user_id = None
        self.ctime = None
        self.utime = None
        self.is_del = None
        super(Ds, self).__init__(session, *args, **kwargs)

    def delete(self):
        super(Ds, self).delete()
        finder = Finder(self.user_id)
        finder.rm_datasource(self.name)

    def update(self, **kwargs):
        super(Ds, self).update(**kwargs)
        if 'params' in kwargs and 'filelist' in kwargs['params']:
            finder = Finder(self.user_id)
            filelist = kwargs['params']['filelist']
            names = [_.name for _ in self.all()]
            for name in names:
                dirs = os.listdir(finder.datasource(name))
                for filename in dirs:
                    if filename not in filelist:
                        finder.rm_table(name, filename)
