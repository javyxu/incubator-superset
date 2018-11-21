# -*- coding: utf-8 -*-
# pylint: disable=C,R,W
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from flask import Flask, jsonify, Response
from flask_restful import reqparse, Resource, Api, request

import superset.models.core as models
import superset.models.helpers as helper
from superset import app, db, utils
from .base import json_error_response

import simplejson as json

api = Api(app)

def json_success(msg, status=200):
    # res = Response(json_msg, status=status, mimetype='application/json')
    return jsonify({"code":status, "result": msg})

# 获取Databases
class Databases(Resource):
    def get(self):
        try:
            session = db.session
            databases = session.query(models.Database).all()
            temp = []
            for database in databases:
                temp.append(database.name)
        except Exception as e:
            return json_error_response(e)
        # return jsonify({"status":"OK","code":"200","message":"","result": { "databases": json.dumps(temp)}})
        return json_success(msg={"databases": json.dumps(temp)})

api.add_resource(Databases, '/api/v1/databases')

# 获取Schemas
class Schemas(Resource):
    def get(self, database_name):
        try:
            session = db.session
            databases = session.query(models.Database).all()
            curdatabase = None
            for database in databases:
                if database_name ==  database.name:
                    curdatabase = database
                    break
            schemas = curdatabase.all_schema_names()
        except Exception as e:
            return json_error_response(e)
        return json_success(msg={"schemas": json.dumps(schemas)})

api.add_resource(Schemas, '/api/v1/databases/<database_name>')

# 获取Tables
class Tables(Resource):
    def get(self, database_name, schema_name=None):
        try:
            session = db.session
            databases = session.query(models.Database).all()
            curdatabase = None
            for database in databases:
                if database_name ==  database.name:
                    curdatabase = database
                    break
            alltables = curdatabase.all_table_names(schema=schema_name)
        except Exception as e:
            return json_error_response(e) 
        return json_success(msg={"tables": json.dumps(alltables)})

api.add_resource(Tables, '/api/v1/databases/<database_name>/<schema_name>')

# 获取ColNames
class ColNames(Resource):
    def get(self, database_name, table_name, schema_name=None):
        try:
            session = db.session
            databases = session.query(models.Database).all()
            curdatabase = None
            for database in databases:
                if database_name ==  database.name:
                    curdatabase = database
                    break
            colnames = curdatabase.get_columns(table_name, schema_name)
            tmp = []
            for colname in colnames:
                d = {}
                for kv in colname:
                    d[kv] = str(colname[kv])
                tmp.append(d)
        except Exception as e:
            return json_error_response(e)
        return json_success(msg={"columns": json.dumps(tmp)})

api.add_resource(ColNames, '/api/v1/databases/<database_name>/<schema_name>/<table_name>')


# 获取Datas
class GetAllData(Resource):
    def get(self, database_name, table_name, schema_name=None):
        try:
            session = db.session
            databases = session.query(models.Database).all()
            curdatabase = None
            for database in databases:
                if database_name ==  database.name:
                    curdatabase = database
                    break
            # if schema_name is not None:
            #     table_name = schema_name + "." + table_name
            datas = curdatabase.get_df('SELECT * FROM {0}'.format(table_name), schema_name)
        except Exception as e:
            return json_error_response(e)
        return json_success(msg={"datas": datas.to_json(orient='records')})

api.add_resource(GetAllData, '/api/v1/sql/getalldata/<database_name>/<schema_name>/<table_name>')

# 执行SQL
class ExecuteSql(Resource):
    def get(self, database_name, sql, schema_name=None):
        try:
            session = db.session
            databases = session.query(models.Database).all()
            curdatabase = None
            for database in databases:
                if database_name ==  database.name:
                    curdatabase = database
                    break
            datas = curdatabase.get_df(sql, schema_name)
        except Exception as e:
            json_error_response(e)
        return json_success(msg=datas.to_json(orient='records'))

api.add_resource(ExecuteSql, '/api/v1/sql/executesql/<database_name>/<sql>')


class GetDatamodel(Resource):
    def get(self):
        try:
            session = db.session
            databases = session.query(models.Database).all()
            curdatabase = None
            for database in databases:
                if database.name ==  "main":
                    curdatabase = database
                    break
            datas = curdatabase.get_df('SELECT * FROM dbs', 'main')
        except Exception as e:
            return json_error_response(e)
        return json_success(msg={"datamodels": datas.to_json(orient='records')})

parser = reqparse.RequestParser()
parser.add_argument('data')
class AddDatamodel(Resource):
    def get(self):
        try:
            args = parser.parse_args()
            data = args['data']
            # return jsonify({"result": data})
            # task = {
            #     'database_name': database_name,
            #     'sqlalchemy_uri': 'postgresql://rasdaman:XXXXXXXXXX@10.0.4.90:5432/RASBASE',
            #     'created_by_fk': 1,
            #     'changed_by_fk': 1,
            #     'password': utils.zlib_compress('xxxxxxxx'),
            #     'cache_timeout': 40,
            #     'extra' : '',
            #     'select_as_create_table_as': True,
            #     'allow_ctas': True,
            #     'expose_in_sqllab' : True,
            #     'force_ctas_schema': '',
            #     'allow_run_async': True,
            #     'allow_run_sync': True,
            #     'allow_dml': True,
            #     'perm':'',
            #     'verbose_name':'',
            #     'impersonate_user' : False,
            #     'allow_multi_schema_metadata_fetch' : False,
            # }
            # dict_rep = task
            dict_rep = helper.json_to_dict(data)
            session = db.session
            models.Database.import_from_dict(session=session, dict_rep=dict_rep)
            session.commit()
        except Exception as e:
            return json_error_response(e)
        return json_success(msg=json.dumps("ok"))

parser = reqparse.RequestParser()
parser.add_argument('id', type=int)
parser.add_argument('data', type=str)
class UpdateDatamodel(Resource):
    def get(self):
        try:
            args = parser.parse_args()
            pk = args['id']
            data = args['data']
            # return jsonify({"result": data})
            # task = {
            #     'database_name': database_name,
            #     'sqlalchemy_uri': 'postgresql://rasdaman:XXXXXXXXXX@10.0.4.90:5432/RASBASE',
            #     'created_by_fk': 1,
            #     'changed_by_fk': 1,
            #     'password': utils.zlib_compress('xxxxxxxx'),
            #     'cache_timeout': 40,
            #     'extra' : '',
            #     'select_as_create_table_as': True,
            #     'allow_ctas': True,
            #     'expose_in_sqllab' : True,
            #     'force_ctas_schema': '',
            #     'allow_run_async': True,
            #     'allow_run_sync': True,
            #     'allow_dml': True,
            #     'perm':'',
            #     'verbose_name':'',
            #     'impersonate_user' : False,
            #     'allow_multi_schema_metadata_fetch' : False,
            # }
            # dict_rep = task
            dict_rep = helper.json_to_dict(data)
            session = db.session
            # o = session.query(models.Database).filter_by(id=pk).first()
            # session.delete(o)
            # session.commit()
            models.Database
            models.Database.import_from_dict(session=session, dict_rep=dict_rep, parent=pk)
            session.commit()
        except Exception as e:
            return json_error_response(e)
        return json_success(msg=json.dumps("ok"))



class DeleteDatamodel(Resource):
    def get(self, id):
        try:
            session = db.session
            args = parser.parse_args()
            # pk = args['id']
            o = session.query(models.Database).filter_by(id=id).first()
            session.delete(o)
            session.commit()
        except Exception as e:
            return json_error_response(e)
        return json_success(msg=json.dumps({"result": "ok"}))

api.add_resource(GetDatamodel, '/api/v1/datamodels')
# http://172.16.151.188:8088/api/v1/datamodel/add?data={%22database_name%22:%22test6%22,%22sqlalchemy_uri%22:%22postgresql://rasdaman:XXXXXXXXXX@10.0.4.90:5432/RASBASE%22,%22created_by_fk%22:1,%22changed_by_fk%22:1,%22password%22:%2212345678%22,%22cache_timeout%22:40,%22extra%22:%22%22,%22select_as_create_table_as%22:true,%22allow_ctas%22:true,%22expose_in_sqllab%22:true,%22force_ctas_schema%22:%22%22,%22allow_run_async%22:true,%22allow_run_sync%22:true,%22allow_dml%22:true,%22perm%22:%22%22,%22verbose_name%22:%22%22,%22impersonate_user%22:false,%22allow_multi_schema_metadata_fetch%22:false}
api.add_resource(AddDatamodel, '/api/v1/datamodels/add')
api.add_resource(UpdateDatamodel, '/api/v1/datamodels/edit')
api.add_resource(DeleteDatamodel, '/api/v1/datamodels/delete/<id>')