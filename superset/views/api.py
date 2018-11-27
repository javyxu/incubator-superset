# -*- coding: utf-8 -*-
# pylint: disable=C,R,W
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import logging

from flask import Flask, jsonify, Response, request
from flask_restful import reqparse, Resource, Api

import superset.models.core as models
import superset.models.helpers as helper
import superset.models.sql_lab as sqllab
from superset import app, db, utils
from .base import json_error_response
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from flask_appbuilder.models.sqla.interface import SQLAInterface
from flask_cors import CORS

import simplejson as json

CORS(app, supports_credentials=True)
CORS(app, resources=r'/api')
api = Api(app)

parser = reqparse.RequestParser()

def json_result(code=0, data=None, msg='success'):
    # res = Response(json_msg, status=status, mimetype='application/json')
    return jsonify({"code":code, "data": data, "msg":msg})

# 获取Databases
class Databases(Resource):
    def get(self):
        try:
            session = db.session
            databases = session.query(models.Database).all()
            temp = []
            for database in databases:
                if database.name != 'main':
                    temp.append(database.name)
        except Exception as e:
            logging.exception(e)
            # return json_error_response(e)
            return json_result(code=500, msg=str(e))
        # return jsonify({"status":"OK","code":"200","message":"","result": { "databases": json.dumps(temp)}})
        return json_result(data={"databases": json.dumps(temp)})

api.add_resource(Databases, '/api/v1/databases')

# 获取Schemas

class Schemas(Resource):
    parser.add_argument('databasename', type=str)
    def get(self):
        try:
            args = parser.parse_args()
            database_name = args['databasename']
            session = db.session
            curdatabase = session.query(models.Database).filter_by(database_name=database_name).first()
            schemas = curdatabase.all_schema_names()
        except Exception as e:
            logging.exception(e)
            # return json_error_response(e)
            return json_result(code=500, msg=str(e))
        return json_result(data={"schemas": json.dumps(schemas)})

api.add_resource(Schemas, '/api/v1/getschemas')

# 获取Tables
class Tables(Resource):
    parser.add_argument('databasename', type=str)
    parser.add_argument('schemaname', type=str)
    def get(self):
        try:
            args = parser.parse_args()
            database_name = args['databasename']
            schema_name = args['schemaname']
            session = db.session
            curdatabase = session.query(models.Database).filter_by(database_name=database_name).first()
            alltables = curdatabase.all_table_names(schema=schema_name)
        except Exception as e:
            logging.exception(e)
            # return json_error_response(e)
            json_result(code=500, msg=str(e)) 
        return json_result(data={"tables": json.dumps(alltables)})

api.add_resource(Tables, '/api/v1/gettables')

# 获取ColNames
class ColNames(Resource):
    parser.add_argument('databasename', type=str)
    parser.add_argument('schemaname', type=str)
    parser.add_argument('tablename', type=str)
    def get(self):
        try:
            args = parser.parse_args()
            database_name = args['databasename']
            schema_name = args['schemaname']
            table_name = args['tablename']
            session = db.session
            curdatabase = session.query(models.Database).filter_by(database_name=database_name).first()
            colnames = curdatabase.get_columns(table_name, schema_name)
            tmp = []
            for colname in colnames:
                d = {}
                for kv in colname:
                    d[kv] = str(colname[kv])
                tmp.append(d)
        except Exception as e:
            logging.exception(e)
            # return json_error_response(e)
            return json_result(code=500, msg=str(e))
        return json_result(data={"columns": json.dumps(tmp)})

api.add_resource(ColNames, '/api/v1/getcolnames')


# 获取Datas
class GetAllData(Resource):
    parser.add_argument('databasename', type=str)
    parser.add_argument('schemaname', type=str)
    parser.add_argument('tablename', type=str)
    def get(self):
        try:
            args = parser.parse_args()
            database_name = args['databasename']
            schema_name = args['schemaname']
            table_name = args['tablename']
            session = db.session
            curdatabase = session.query(models.Database).filter_by(database_name=database_name).first()
            datas = curdatabase.get_df('SELECT * FROM {0}'.format(table_name), schema_name)
        except Exception as e:
            logging.exception(e)
            # return json_error_response(e)
            return json_result(code=500, msg=str(e))
        return json_result(data={"datas": datas.to_json(orient='records')})

api.add_resource(GetAllData, '/api/v1/sql/getalldata')

# 执行SQL
class ExecuteSql(Resource):
    parser.add_argument('databasename', type=str)
    parser.add_argument('schemaname', type=str)
    parser.add_argument('sql', type=str)
    def get(self):
        try:
            args = parser.parse_args()
            database_name = args['databasename'] 
            schema_name = args['schemaname']
            sql = args['sql']
            session = db.session
            curdatabase = session.query(models.Database).filter_by(database_name=database_name).first()
            datas = curdatabase.get_df(sql, schema_name)
        except Exception as e:
            logging.exception(e)
            # json_error_response(e)
            return json_result(code=500, msg=str(e))
        return json_result(data=datas.to_json(orient='records'))

api.add_resource(ExecuteSql, '/api/v1/sql/executesql')


class SavedSql(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('data', type=str)
    def get(self):
        try:
            args = parser.parse_args()
            data = args['data']
            # task = {
            #     'db_id': 1,
            #     'schema': 'main',
            #     'label': 'test2',
            #     'description' : 'test1',
            #     'sql' : 'select * from dbs',
            # }
            # dict_rep = task
            dict_rep = dict_rep = helper.json_to_dict(data)
            datamodel = sqllab.SavedQuery()
            for kv in dict_rep:
                 setattr(datamodel, kv, dict_rep[kv])
            db.session.add(datamodel)
            db.session.commit()
        except Exception as e:
            logging.exception(e)
            # json_error_response(e)
            return json_result(code=500, msg=str(e))
        return json_result(data=None)
        
api.add_resource(SavedSql, '/api/v1/sql/savedsql')


class GetDatamodel(Resource):
    def get(self):
        try:
            session = db.session
            curdatabase = session.query(models.Database).filter_by(database_name='main').first()
            datas = curdatabase.get_df('SELECT * FROM dbs WHERE database_name != "main"', 'main')
        except Exception as e:
            logging.exception(e)
            # return json_error_response(e)
            return json_result(code=500, msg=str(e))
        return json_result(data={"datamodels": datas.to_json(orient='records')})
api.add_resource(GetDatamodel, '/api/v1/datamodels')


# test conn
class TestConnection(Resource):
    # parser.add_argument('uri')
    # parser.add_argument('name')
    # parser.add_argument('impersonate_use')
    # parser.add_argument('extra')
    def post(self):
        """Tests a sqla connection"""
        try:
            data = request.json
            # data2 = json.dumps(data1)
            # data = json.loads(data2)
            # username = g.user.username if g.user is not None else None
            username = ''
            # # data = json.loads(request.form)
            uri = data.get('uri')
            # # uri = args['uri']
            db_name = data.get('name')
            # # db_name = args['name']
            impersonate_user = data.get('impersonate_user')
            # # impersonate_use = args['impersonate_use']
            database = None
            if db_name:
                database = (
                    db.session
                    .query(models.Database)
                    .filter_by(database_name=db_name)
                    .first()
                )
                if database and uri == database.safe_sqlalchemy_uri():
                    # the password-masked uri was passed
                    # use the URI associated with this database
                    uri = database.sqlalchemy_uri_decrypted

            configuration = {}

            if database and uri:
                url = make_url(uri)
                db_engine = models.Database.get_db_engine_spec_for_backend(
                    url.get_backend_name())
                db_engine.patch()

                masked_url = database.get_password_masked_url_from_uri(uri)
                logging.info('Superset.testconn(). Masked URL: {0}'.format(masked_url))

                configuration.update(
                    db_engine.get_configuration_for_impersonation(uri,
                                                                  impersonate_user,
                                                                  username),
                )
            
            str_extras = data.get('extras')
            extras_json = json.loads(str_extras)
            engine_params = (
                extras_json.get('engine_params', {})
            )
            connect_args = engine_params.get('connect_args')

            if configuration:
                connect_args['configuration'] = configuration

            engine = create_engine(uri, **engine_params)
            engine.connect()
            # return json_success(json.dumps(engine.table_names(), indent=4))
        except Exception as e:
            logging.exception(e)
            # return json_error_response((
            #     'Connection failed!\n\n'
            #     'The error message returned was:\n{}').format(e))
            return json_result(code=500, msg=str(e))
        return json_result(data=json.dumps(engine.table_names()))
        # return json_result(data=tmp)
api.add_resource(TestConnection, '/api/v1/testconn')


class AddDatamodel(Resource):
    # parser.add_argument('data')
    def post(self):
        try:
            # database_name = request.form['database_name']
            # args = parser.parse_args()
            # data = args['data']
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
            # dict_rep = request.form
            dict_rep = helper.json_to_dict(json.dumps(request.form))
            session = db.session
            models.Database.import_from_dict(session=session, dict_rep=dict_rep)
            session.commit()
        except Exception as e:
            logging.exception(e)
            # return json_error_response(e)
            return json_result(code=500, msg=str(e))
        return json_result(data=dict_rep)
# http://172.16.151.188:8088/api/v1/datamodel/add?data={%22database_name%22:%22test6%22,%22sqlalchemy_uri%22:%22postgresql://rasdaman:XXXXXXXXXX@10.0.4.90:5432/RASBASE%22,%22created_by_fk%22:1,%22changed_by_fk%22:1,%22password%22:%2212345678%22,%22cache_timeout%22:40,%22extra%22:%22%22,%22select_as_create_table_as%22:true,%22allow_ctas%22:true,%22expose_in_sqllab%22:true,%22force_ctas_schema%22:%22%22,%22allow_run_async%22:true,%22allow_run_sync%22:true,%22allow_dml%22:true,%22perm%22:%22%22,%22verbose_name%22:%22%22,%22impersonate_user%22:false,%22allow_multi_schema_metadata_fetch%22:false}
api.add_resource(AddDatamodel, '/api/v1/datamodels/add')


class UpdateDatamodel(Resource):
    # parser.add_argument('id', type=int)
    # parser.add_argument('data', type=str)
    def post(self, id):
        try:
            # args = parser.parse_args()
            # pk = args['id']
            # data = args['data']
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
            # dict_rep = helper.json_to_dict(data)
            dict_rep = helper.json_to_dict(json.dumps(request.form))
            session = db.session
            curdatabase = session.query(models.Database).filter_by(id=int(id)).first()
            for kv in dict_rep:
                 setattr(curdatabase, kv, dict_rep[kv])
            db.session.add(curdatabase)
            db.session.commit()
            session.commit()
        except Exception as e:
            logging.exception(e)
            # return json_error_response(e)
            return json_result(code=500, msg=str(e))
        return json_result(data=None)
api.add_resource(UpdateDatamodel, '/api/v1/datamodels/edit/<id>')

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
            logging.exception(e)
            # return json_error_response(e)
            return json_result(code=500, msg=str(e))
        return json_result(data=None)
api.add_resource(DeleteDatamodel, '/api/v1/datamodels/delete/<id>')