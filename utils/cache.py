#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

__author__ = 'tong'


class Cache(object):
    def __init__(self):
        from diskcache import Cache
        self.cache = Cache('/tmp/navan')
        self.cache.stats(enable=True)

    def get(self, *args):
        return self.cache.get(':'.join(args))

    def set(self, *args, **kwargs):
        expire = kwargs.get('expire')
        if len(args) < 2:
            raise Exception('cache set must contain `key` and `value`')
        key, value = args[:-1], args[-1]
        key = ':'.join(key)
        return self.cache.set(key, value, expire)

    def get_json(self, *args):
        ret = self.get(*args)
        if not ret:
            return ret
        return json.loads(ret)

    def set_json(self, *args, **kwargs):
        args = list(args)
        args[-1] = json.dumps(args[-1])
        return self.set(*args, **kwargs)

cache = Cache()
