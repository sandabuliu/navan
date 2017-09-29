#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import uuid
import base64
import pickle
import logging
import traceback
import tornado.web
import tornado.ioloop
import tornado.httpserver
from random import randint
from functools import wraps
from Crypto.Cipher import AES
from types import GeneratorType
from tornado.gen import coroutine, Task
from tornado.web import RequestHandler, StaticFileHandler, HTTPError, MissingArgumentError

__author__ = 'tong'


root_path = os.path.abspath(os.path.dirname(os.path.abspath(__file__)))


class Server(object):
    @property
    def logger(self):
        return logging.getLogger('runtime')

    def sig_handler(self, sig, frame):
        self.logger.info('Caught signal: %s\nTraceback (most recent call last):\n%s' %
                         (sig, ''.join(traceback.format_stack(frame))))
        tornado.ioloop.IOLoop.instance().add_callback(self.shutdown)

    def shutdown(self):
        import time
        deadline = time.time() + 40 * 60

        self.logger.info('Stopping http server')
        self.server.stop()
        self.logger.info('Will shutdown in %s seconds ...', (deadline-time.time()))
        io_loop = tornado.ioloop.IOLoop.instance()

        def stop():
            io_loop.stop()
            self.logger.info('Shutdown')

        def stop_loop():
            now = time.time()
            if now < deadline and io_loop._callbacks:
                self.logger.info('seconds: %s, callbacks: %s' %
                                 (deadline - now, io_loop._callbacks))
                io_loop.add_timeout(now + 1, stop_loop)
            else:
                stop()
        stop_loop()

    def __init__(self, app, port=20720):
        self.port = port
        sys.stderr.write("listen server on port %s ...\n" % self.port)
        self.server = tornado.httpserver.HTTPServer(app)

    def start(self, num_processes=1):
        import signal
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)

        self.server.bind(self.port)
        self.server.start(num_processes)
        tornado.ioloop.IOLoop.instance().start()


class Application(tornado.web.Application):
    def __init__(self, api, default_host="", transforms=None, **settings):
        self.handlers = []
        self.base_dir = os.path.dirname(os.path.join(root_path, api))
        self.load_handlers(api)
        super(Application, self).__init__(self.handlers, default_host, transforms, **settings)

    def load(self, filename):
        handler, ext = os.path.splitext(filename)
        route_path = handler[len(self.base_dir):]
        module_name = route_path.strip('/').replace('/', '.')
        __import__(module_name)
        module = sys.modules.get(module_name)
        for name in dir(module):
            if name.startswith('_'):
                continue
            obj = getattr(module, name)
            if not hasattr(obj, '__bases__'):
                continue
            if BaseHandler not in obj.__bases__:
                continue

            route_path = os.path.dirname(route_path)
            route_path = '%s/%s' % (route_path, obj.__name__.lower().split('handler')[0])
            self.handlers.append((route_path, obj))
            print 'routing uri %s -> %s' % (route_path, obj)

    def load_handlers(self, api):
        __import__(api)

        api_dir = os.path.join(root_path, api)
        for parent, dirnames, filenames in os.walk(api_dir):
            for name in filenames:
                if not name.endswith('.py'):
                    continue
                if name.startswith('_'):
                    continue

                self.load(os.path.join(parent, name))
        self.handlers.append((r'^/(.*?)$', StaticFileHandler, {"path": os.path.join(root_path, "static"),
                                                               "default_filename": "index.html"}))

    def log_request(self, handler):
        if hasattr(handler, 'log_request'):
            handler.log_request()
        else:
            super(Application, self).log_request(handler)


class LogTracer(logging.Filter):
    def __init__(self, name=''):
        super(LogTracer, self).__init__(name)
        self.trace_id = None

    def filter(self, record):
        record.trace_id = self.trace_id
        return True


class BaseHandler(RequestHandler):
    api_logger = logging.getLogger('api')
    logger = logging.getLogger('runtime')
    log_tracer = LogTracer()
    logger.addFilter(log_tracer)
    api_logger.addFilter(log_tracer)
    logging.getLogger('query').addFilter(log_tracer)

    AES_OBJ = AES.new(str(uuid.uuid4())[:16])
    NEED_AUTHED = True

    def __init__(self, application, request, **kwargs):
        super(BaseHandler, self).__init__(application, request, **kwargs)
        self.request_start_time = 0
        self.process_start_time = 0
        self.process_end_time = 0
        self.status_code = 200
        self.result = {'message': ''}
        self.trace_id = None
        self.user_id = None
        self.args = None
        for method in self.SUPPORTED_METHODS:
            method = method.lower()
            method_func = getattr(self, method)
            if method_func:
                setattr(self, method, self.handler_wrap(method_func))

    def handler_wrap(self, func):
        @coroutine
        @wraps(func)
        def _func(*args, **kwargs):
            try:
                yield Task(self.run, func, *args, **kwargs)
                self.do_response()
            except Exception, e:
                self.logger.error(e, exc_info=True)
        return _func

    @coroutine
    def run(self, func, *args, **kwargs):
        self.process_start_time = time.time()
        try:
            if self.NEED_AUTHED:
                self.auth()
            ret = func(*args, **kwargs)
            if isinstance(ret, GeneratorType):
                for item in ret:
                    yield item
        except HTTPError, e:
            self.response(e.status_code, str(e))
            self.logger.error(e, exc_info=True)
        except Exception, e:
            self.response(500, "Interal error: %s" % e)
            self.logger.error(e, exc_info=True)
        finally:
            self.process_end_time = time.time()

    def do_response(self):
        self.set_header("Content-Type", "application/json;charset=utf-8")
        self.add_header("Connection", "keep-alive")
        self.set_status(self.status_code, self.result.get('message'))
        self.write(json.dumps(self.result, cls=JSONEncoder, allow_nan=False))
        self.finish('\n')

    def prepare(self):
        self.request_start_time = time.time()
        self.process_start_time = self.request_start_time
        self.process_end_time = self.request_start_time
        self.status_code = 200
        self.trace_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, "%s_%s_%s" %
                                       (self.request.path, time.time(), randint(0, 100000))))
        self.result = {'message': ''}
        self.log_tracer.trace_id = self.trace_id

    def response(self, status_code=200, message='', **kwargs):
        self.status_code = status_code
        if message:
            kwargs['message'] = message
        self.result = kwargs

    def location(self, name):
        if name == 'args':
            return {key: self.get_argument(key) for key in self.request.arguments}
        elif name == 'body':
            return json.loads(self.request.body or '{}')

    def parse_args(self, arg_list):
        results = {}
        for arg in arg_list:
            location = arg.get('location')
            required = arg.get('required')

            name = arg.get('name')
            cast = arg.get('cast')
            para = self.location(location)
            if name not in para:
                if 'default' in arg:
                    para[name] = arg['default']
                elif required:
                    raise MissingArgumentError(name)
                else:
                    continue
            value = para.get(name)
            if cast:
                try:
                    value = cast(value)
                except Exception, e:
                    raise HTTPError(400, "Invalid %s(%s): %s" % (str(cast), name, e))

            results[name] = value
        self.args = results
        return results

    def log_request(self):
        request_end_time = time.time()
        pending = int((self.process_start_time - self.request_start_time) * 1000)
        process = int((self.process_end_time - self.process_start_time) * 1000)
        tornado = int((request_end_time - self.process_end_time) * 1000)
        ip = self.request.headers.get('X-Real-Ip') or self.request.remote_ip
        uri = self.request.path
        params = '&'.join(["%s=%s" % (key, self.get_argument(key)) for key in self.request.arguments])
        if uri == '/api/file' and self.request.method == 'POST':
            body = ['filename=%s&content_type=%s' % (f['filename'], f['content_type'])
                    for f in self.request.files.get('file')]
            body = ';'.join(body)
        else:
            body = self.request.body
        log = u'[{port}] [{ip}] [{meth} {uri}] [{params}] [{body}] [{status_code}] {pending} {process} {tornado} {all}'.format(
            port=self.application.listening_port,
            ip=ip, uri=uri, params=params,
            status_code=self.status_code, body=body,
            pending=pending, process=process,
            tornado=tornado, meth=self.request.method,
            all=int((request_end_time - self.request_start_time) * 1000)
        )

        if self.status_code != 200:
            self.api_logger.error(log)
        else:
            self.api_logger.info(log)

    def set_cookie(self, name, value, domain=None, expires=None, path="/",
                   expires_days=None, **kwargs):
        value = pickle.dumps(value)
        value = self.AES_OBJ.encrypt(value+' ' * (16 - len(value) % 16))
        super(BaseHandler, self).set_cookie(name, base64.b64encode(value))

    def get_cookie(self, name, default=None):
        value = super(BaseHandler, self).get_cookie(name, default)
        if value == default:
            return value
        value = base64.b64decode(value)
        value = self.AES_OBJ.decrypt(value)
        value = pickle.loads(value)
        return value

    def auth(self):
        from model import DBMeta
        if self.request.path == '/api/login':
            return
        try:
            user = self.get_cookie('user')
        except Exception, e:
            self.logger.error(e, exc_info=True)
            raise UnAuthentication()

        if not user:
            raise UnAuthentication()
        if time.time() - user.get('access', 0) > 60 * 60 * 24:
            raise AuthExpire()
        try:
            db = DBMeta()
            db = db.user(**user).auth()
            self.user_id = db['id']
        except Exception:
            raise UnAuthentication()

    def data_received(self, chunk):
        pass


class UnAuthentication(HTTPError):
    def __init__(self):
        super(UnAuthentication, self).__init__(401, 'Auth failed!')


class AuthExpire(HTTPError):
    def __init__(self):
        super(AuthExpire, self).__init__(401, 'Auth expired!')


class JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        import pandas
        from decimal import Decimal
        from datetime import datetime, date, time

        if obj == type(None):
            return None
        if isinstance(obj, type):
            return obj.__name__
        if isinstance(obj, type(pandas.NaT)):
            return None
        if isinstance(obj, datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        if isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        if isinstance(obj, time):
            return obj.strftime('%H:%M:%S')
        if isinstance(obj, Decimal):
            return float(str(obj))
        return super(JSONEncoder, self).default(obj)

formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] [%(trace_id)s] %(message)s')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
BaseHandler.logger.addHandler(handler)
BaseHandler.api_logger.addHandler(handler)
