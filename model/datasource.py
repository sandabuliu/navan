#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import shutil
from sqlalchemy import Column
from sqlalchemy.dialects.mysql import INTEGER, VARCHAR, TINYINT, TIMESTAMP
from base import Base, MetaBase, AESJson

from query.constants import FILE_PATH


__author__ = 'tong'


class DsModel(Base):
    __tablename__ = 'datasource'

    id = Column('id', INTEGER(display_width=11, unsigned=True), primary_key=True, nullable=False)
    name = Column('name', VARCHAR(length=255), nullable=False)
    type = Column('type', VARCHAR(length=64), nullable=False)
    params = Column('params', AESJson())
    user_id = Column('user_id', INTEGER(display_width=11), nullable=False)
    ctime = Column('ctime', TIMESTAMP(), nullable=False)
    utime = Column('utime', TIMESTAMP(), nullable=False)
    is_del = Column('is_del', TINYINT(display_width=4, unsigned=True), nullable=False, default=0)


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
        path = os.path.join(FILE_PATH, str(self.user_id), 'datasource', self.name)
        trash = os.path.join(FILE_PATH, str(self.user_id), 'trash')
        if not os.path.exists(trash):
            os.makedirs(trash)
        shutil.move(path, trash)
