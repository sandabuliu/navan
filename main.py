#!/usr/bin/env python
# -*- coding: utf-8 -*-

from server import Application, Server
from tornado.options import define, options, parse_command_line


__author__ = 'tong'

define("port", default="20720", help="service listening port")


if __name__ == '__main__':
    parse_command_line()
    port = int(options.port)
    application = Application('api', **{'is_debug': True})
    application.listening_port = port

    server = Server(application, port)
    server.start(1)
