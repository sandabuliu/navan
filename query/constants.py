#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from datetime import datetime, date, time, timedelta

__author__ = 'tong'


FILE_PATH = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'filesystem'))

types = {
    'Bytes': bytes,
    'CType': type,
    'Date': date,
    'DateTime': datetime,
    'Decimal': float,
    'Map': dict,
    'Null': type(None),
    'String': basestring,
    'Time': time,
    'TimeDelta': timedelta,
    'Tuple': tuple,
    'TypeSet': set,
    'bool': bool,
    'bytes': bytes,
    'char': str,
    'complex128': complex,
    'complex64': complex,
    'complex[float32]': complex,
    'complex[float64]': complex,
    'date': date,
    'datetime': datetime,
    'double': float,
    'float': float,
    'float16': float,
    'float32': float,
    'float64': float,
    'int': int,
    'int16': int,
    'int32': int,
    'int64': int,
    'int8': int,
    'intptr': int,
    'null': type(None),
    'object': object,
    'real': float,
    'string': basestring,
    'time': time,
    'timedelta': timedelta,
}