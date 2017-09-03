#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from sqlalchemy import Column
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.dialects.mysql import INTEGER, TINYINT, TIMESTAMP, TEXT, VARCHAR
from base import Base, MetaBase

__author__ = 'tong'


class VTableModel(Base):
    __tablename__ = 'vtable'

    id = Column('id', INTEGER(display_width=11, unsigned=True), primary_key=True, nullable=False)
    name = Column('name', VARCHAR(length=255), nullable=False)
    ds_id = Column('ds_id', INTEGER(display_width=11, unsigned=True), nullable=False)
    table_json = Column('query', TEXT(), nullable=False)
    user_id = Column('user_id', INTEGER(display_width=11), nullable=False)
    ctime = Column('ctime', TIMESTAMP(), nullable=False)
    utime = Column('utime', TIMESTAMP(), nullable=False)
    is_del = Column('is_del', TINYINT(display_width=4, unsigned=True), nullable=False, default=0)

    @hybrid_property
    def query(self):
        return json.loads(self.table_json)

    @query.setter
    def query(self, value):
        self.table_json = json.dumps(value)


class VTable(MetaBase):
    object = VTableModel
