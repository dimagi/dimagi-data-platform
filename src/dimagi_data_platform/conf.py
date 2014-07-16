'''
Created on Jun 6, 2014

@author: mel
'''

import json
import logging
import os

from playhouse.postgres_ext import PostgresqlExtDatabase


logger = logging.getLogger('dimagi_data_platform')

'''
SYSTEM CONFIGURATION - config_system.json
'''
with open('config_system.json', 'r') as f:
    global _json_conf
    _json_conf = json.loads(f.read())['data_platform']
if not _json_conf:
    raise RuntimeError('Could not load system config file!') 

DB_USER = _json_conf['database']['user']
DB_PASS = _json_conf['database']['pass']
DB_NAME = _json_conf['database']['dbname']
DB_HOST = _json_conf['database']['host']
DB_PORT = _json_conf['database']['port']

CC_USER = _json_conf['commcare_export']['username']

# use paths for these or the R script won't find them
TMP_FILES_DIR = _json_conf['directories']['tmp_files']
OUTPUT_DIR = _json_conf['directories']['output']
INPUT_DIR = _json_conf['directories']['input']

SQLALCHEMY_DB_URL = 'postgresql://%s:%s@%s:%s/%s' % (DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
PEEWEE_DB_CON = PostgresqlExtDatabase(DB_NAME, **{'host': DB_HOST, 'password': DB_PASS, 'user': DB_USER, 'port':DB_PORT})

AWS_S3_INPUT_URL = _json_conf['s3']['input_url']
AWS_S3_OUTPUT_URL = _json_conf['s3']['output_url']


'''
RUN CONFIGURATION - config_run.json
'''
with open(os.path.join(INPUT_DIR,'config_run.json'), 'r') as f:
    global _run_conf
    _run_conf = json.loads(f.read())['data_platform']
if not _run_conf:
    raise RuntimeError('Could not load run config file!') 

DOMAIN_CONF_JSON = _run_conf['domains']



