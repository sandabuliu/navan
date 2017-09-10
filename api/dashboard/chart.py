#!/usr/bin/env python
# -*- coding: utf-8 -*-

from model import DBMeta
from server import BaseHandler
from query.query import Query
from query.connector import Connector

from api.util import chart_data

__author__ = 'tong'


class ChartListHandler(BaseHandler):
    def get(self):
        args = self.parse_args([
            {'name': 'dashboard', 'required': True, 'location': 'args'}
        ])

        dbmeta = DBMeta(self.user_id)
        dashboard = dbmeta.dashboard(name=args['dashboard']).first()
        if not dashboard:
            self.response(404, message='不存在的仪表盘')
            return

        charts = dbmeta.chart('name', dashboard_id=dashboard.id).all()
        self.response(**{'total': len(charts), 'charts': [c.name for c in charts]})


class ChartHandler(BaseHandler):
    def get(self):
        args = self.parse_args([
            {'name': 'dashboard', 'required': True, 'location': 'args'},
            {'name': 'chart', 'required': True, 'location': 'args'}
        ])

        dbmeta = DBMeta(self.user_id)
        dashboard = dbmeta.dashboard('id', name=args['dashboard']).single()
        chart = dbmeta.chart(dashboard_id=dashboard.id, name=args['chart']).single()
        ds = dbmeta.datasource(id=chart.ds_id).single()
        connector = Connector(user_id=self.user_id, type=ds.type, db=ds.name, **ds.params)
        query = Query.load(chart.query, connector)

        result = query.execute()
        fields = [_['name'] for _ in chart.y_fields if _]
        data = chart_data(result, chart.x_fields, fields)
        self.response(ds_id=ds.id, table=chart.table, type=chart.type, sql=query.sql,
                      xFields=chart.x_fields, yFields=chart.y_fields, **data)
