#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import logging.config
from server import Application, Server
from tornado.options import define, options, parse_command_line


__author__ = 'tong'

define("port", default="20720", help="service listening port")
define("init", type=bool, help='build env')


if __name__ == '__main__':
    parse_command_line()
    if options.init:
        from model.base import Base, engine
        Base.metadata.create_all(engine)
    else:
        if not os.path.exists('log'):
            os.makedirs('log')
        logging.config.fileConfig('logging.conf')

        port = int(options.port)
        application = Application('api')
        application.listening_port = port

        server = Server(application, port)
        server.start(10)
