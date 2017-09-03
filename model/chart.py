#!/usr/bin/env python
# -*- coding: utf-8 -*-

from model.base import Dict, List

from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, TINYINT, TIMESTAMP, VARCHAR, CHAR
from base import Base, MetaBase

__author__ = 'tong'


class ChartModel(Base):
    __tablename__ = 'chart'

    id = Column('id', INTEGER(display_width=11, unsigned=True), primary_key=True, nullable=False)
    name = Column('name', VARCHAR(length=255), nullable=False)
    dashboard_id = Column('dashboard_id', INTEGER(display_width=11, unsigned=True), nullable=False)
    ds_id = Column('ds_id', INTEGER(display_width=11, unsigned=True), nullable=False)
    table = Column('table', VARCHAR(length=255), nullable=False)
    query = Column('query', Dict(), nullable=False)
    x_fields = Column('x_fields', List(), nullable=False)
    y_fields = Column('y_fields', List(), nullable=False)
    type = Column('type', CHAR(length=32), nullable=False)
    user_id = Column('user_id', INTEGER(display_width=11), nullable=False)
    ctime = Column('ctime', TIMESTAMP(), nullable=False)
    utime = Column('utime', TIMESTAMP(), nullable=False)
    is_del = Column('is_del', TINYINT(display_width=4, unsigned=True), nullable=False, default=0)


class Chart(MetaBase):
    object = ChartModel
