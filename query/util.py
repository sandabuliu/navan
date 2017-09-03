#!/usr/bin/env python
# -*- coding: utf-8 -*-


__author__ = 'tong'


def token():
    from uuid import uuid4
    return str(uuid4()).replace('_', '')
