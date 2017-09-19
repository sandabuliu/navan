#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
from model import DBMeta
from server import BaseHandler

from query.query import Query
from query.connector import Connector
from query.clause import Table, Column, Text, Condition

from api.util import chart_data, function

__author__ = 'tong'


class ChartHandler(BaseHandler):
    def get(self):
        args = self.parse_args([
            {'name': 'ds_id', 'required': True, 'location': 'args', 'cast': int},
            {'name': 'table', 'required': True, 'location': 'args'},
            {'name': 'xFields', 'required': True, 'location': 'args', 'cast': json.loads},
            {'name': 'yFields', 'required': True, 'location': 'args', 'cast': json.loads},
            {'name': 'filters', 'required': False, 'location': 'args', 'cast': json.loads, 'defalt': '[]'},
            {'name': 'type', 'required': True, 'location': 'args'}
        ])

        self.dbmeta = DBMeta(self.user_id)
        ds = self.dbmeta.datasource(id=args['ds_id']).single()
        connector = Connector(user_id=self.user_id, type=ds.type, db=ds.name, **ds.params)

        query = self.query
        self.logger.info('%s' % query.json())
        query.deepbind(connector)
        result = query.execute()

        fields = [_['name'][0] for _ in self.args['yFields'] if _['name']]
        self.response(sql=query.sql, **chart_data(result, args['xFields'], fields))
        del self.dbmeta

    def post(self):
        args = self.parse_args([
            {'name': 'name', 'required': True, 'location': 'body'},
            {'name': 'dashboards', 'required': True, 'location': 'body'},
            {'name': 'ds_id', 'required': True, 'location': 'body', 'cast': int},
            {'name': 'table', 'required': True, 'location': 'body'},
            {'name': 'xFields', 'required': True, 'location': 'body'},
            {'name': 'yFields', 'required': True, 'location': 'body'},
            {'name': 'type', 'required': True, 'location': 'body'}
        ])
        self.dbmeta = DBMeta(self.user_id)
        query = self.query.json()

        y_fields = [{'name': _['name'][0], 'aggr_func': _['name'][1]} for _ in self.args['yFields'] if _['name']]

        dsbd = self.dbmeta.dashboard('id', 'name')
        dsbds = dsbd.filter(dsbd.column('name').in_(args['dashboards'])).all()
        dsbds = {_.id: _.name for _ in dsbds}
        for dsid, dsbdname in dsbds.items():
            ret = self.dbmeta.chart('dashboard_id', name=args['name'], dashboard_id_id=dsid).all()
            if ret:
                self.response(412, message=u'仪表盘 %s 中已存在名字为 %s 的图表' % (dsbdname, args['name']))
                return

        chart = self.dbmeta.chart(name=args['name'], ds_id=args['ds_id'], table=args['table'], query=query,
                                  x_fields=args['xFields'], y_fields=y_fields, type=args['type'])
        for dashboard in args['dashboards']:
            dashboard = self.dbmeta.dashboard(name=dashboard).first()
            if not dashboard:
                continue
            chart.dashboard_id = dashboard.id
        chart.insert()

        self.dbmeta.commit()
        self.response(message='success')
        del self.dbmeta

    def delete(self):
        args = self.parse_args([
            {'name': 'dashboard', 'required': True, 'location': 'args'},
            {'name': 'chart', 'required': True, 'location': 'args'}
        ])
        dbmeta = DBMeta(self.user_id)
        dsbd = dbmeta.dashboard('id', name=args['dashboard']).single()
        dbmeta.chart(name=args['chart'], dashboard_id=dsbd.id).delete()
        dbmeta.commit()
        self.response(message='success')

    def put(self):
        args = self.parse_args([
            {'name': 'name', 'required': True, 'location': 'body'},
            {'name': 'dashboard', 'required': True, 'location': 'body'},
            {'name': 'ds_id', 'required': True, 'location': 'body', 'cast': int},
            {'name': 'table', 'required': True, 'location': 'body'},
            {'name': 'xFields', 'required': True, 'location': 'body'},
            {'name': 'yFields', 'required': True, 'location': 'body'},
            {'name': 'type', 'required': True, 'location': 'body'}
        ])

        self.dbmeta = DBMeta(self.user_id)
        query = self.query.json()

        dashboard = self.dbmeta.dashboard('id', name=args['dashboard']).single()
        y_fields = [{'name': _['name'][0], 'aggr_func': _['name'][1]} for _ in self.args['yFields'] if _['name']]
        chart = self.dbmeta.chart(name=args['name'], dashboard_id=dashboard.id)
        chart.update(
            ds_id=args['ds_id'], table=args['table'], query=query,
            x_fields=args['xFields'], y_fields=y_fields, type=args['type']
        )
        self.dbmeta.commit()
        self.response(message='success')

    @property
    def where(self):
        wheres = []
        for condition in self.args.get('filters', []):
            if not condition['name'] or not condition['operator']:
                continue
            name = condition['name']
            operator = condition['operator']
            if condition.get('value_type', 'value') == 'value':
                value = condition['value']
            else:
                value = Text(condition['value'])
            c = Condition(Column(name), operator, value)
            wheres.append(c.json())
        return wheres

    @property
    def table(self):
        chart = self.dbmeta.vtable(name=self.args['table'], ds_id=self.args['ds_id']).first()
        if chart:
            return chart.query
        return Table(self.args['table']).json()

    @property
    def columns(self):
        fields = [_['name'] for _ in self.args['yFields'] if _['name']]
        result = [function(func_name, name) for name, func_name in fields]
        group_by = self.group_by
        if group_by:
            return group_by+result
        return result

    @property
    def group_by(self):
        if not self.args['xFields']:
            return None
        return [Column(_).json() for _ in self.args['xFields']]

    @property
    def query(self):
        table = self.table
        if table['type'] == 'table':
            return Query(table=self.table, columns=self.columns, where=self.where, group_by=self.group_by, limit=500)
        else:
            query = Query.load(self.table)
            name = self.args['table']
            query.alias(name)
            return Query(table=name, columns=self.columns, where=self.where,
                         group_by=self.group_by, limit=500).bind(query)
