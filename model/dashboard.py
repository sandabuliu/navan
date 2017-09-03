#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import Column
from sqlalchemy.sql import text
from sqlalchemy.sql.schema import DefaultClause
from sqlalchemy.dialects.mysql import INTEGER, CHAR, TINYINT, TIMESTAMP
from base import Base, MetaBase

__author__ = 'tong'


class DashboardModel(Base):
    __tablename__ = 'dashboard'

    id = Column('id', INTEGER(display_width=11, unsigned=True), primary_key=True, nullable=False)
    name = Column('name', CHAR(length=64))
    user_id = Column('user_id', INTEGER(display_width=11), nullable=False)
    ctime = Column('ctime', TIMESTAMP(), nullable=False, server_default=DefaultClause(text('CURRENT_TIMESTAMP')))
    utime = Column('utime', TIMESTAMP(), nullable=False, server_default=DefaultClause(text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')))
    is_del = Column('is_del', TINYINT(display_width=4, unsigned=True), nullable=False, server_default=DefaultClause(text('0')))


class Dashboard(MetaBase):
    object = DashboardModel
