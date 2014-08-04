'''
Created on Jun 6, 2014

@author: mel
'''

import json
import logging
from playhouse.postgres_ext import PostgresqlExtDatabase


logger = logging.getLogger('dimagi_data_platform')

'''
SYSTEM CONFIGURATION - config_system.json
Configures file system locations, database connection and remote accounts
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

# Commcare HQ user
CC_USER = _json_conf['commcare_export']['username']

# the config file should specify full paths for these
INPUT_DIR = _json_conf['directories']['input']
TMP_FILES_DIR = _json_conf['directories']['tmp_files']

SQLALCHEMY_DB_URL = 'postgresql://%s:%s@%s:%s/%s' % (DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
PEEWEE_DB_CON = PostgresqlExtDatabase(DB_NAME, **{'host': DB_HOST, 'password': DB_PASS, 'user': DB_USER, 'port':DB_PORT})

# s3 file storage locations
AWS_S3_INPUT_URL = _json_conf['s3']['input_url']
AWS_S3_OUTPUT_URL = _json_conf['s3']['output_url']

'''
RUN CONFIGURATION - config_run.json
Configures domains to update.
'''
with open('config_run.json', 'r') as f:
    global _run_conf
    _run_conf = json.loads(f.read())['data_platform']
if not _run_conf:
    raise RuntimeError('Could not load run config file!') 

DOMAIN_CONF_JSON = _run_conf['domains']



