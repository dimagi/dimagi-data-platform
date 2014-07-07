'''
Created on Jun 6, 2014

@author: mel
'''

import json
import logging

from playhouse.postgres_ext import PostgresqlExtDatabase

logger = logging.getLogger('dimagi_data_platform')

with open('config.json', 'r') as f:
    global _json_conf
    _json_conf = json.loads(f.read())['data_platform']
if not _json_conf:
    raise RuntimeError('Could not load config file!') 

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

REPORTS = [r['name'] for r in _json_conf['reports']]

AWS_S3_OUTPUT_URL = _json_conf['s3']['output_url']

DOMAIN_CONF_JSON = _json_conf['domains']


SQLALCHEMY_DB_URL = 'postgresql://%s:%s@%s:%s/%s' % (DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
PEEWEE_DB_CON = PostgresqlExtDatabase(DB_NAME, **{'host': DB_HOST, 'password': DB_PASS, 'user': DB_USER, 'port':DB_PORT})
