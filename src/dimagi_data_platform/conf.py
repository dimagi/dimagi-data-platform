'''
Created on Jun 6, 2014

@author: mel
'''

from peewee import PostgresqlDatabase
import json

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

DOMAINS = [d['name'] for d in _json_conf['domains']]

TMP_FILES_DIR = _json_conf['directories']['tmp_files']
OUTPUT_DIR = _json_conf['directories']['output']

REPORTS = [r['name'] for r in _json_conf['reports']]

AWS_S3_OUTPUT_URL = _json_conf['s3']['output_url']


# THIS IS RIDICULOUS. PICK ONE.
SQLALCHEMY_DB_URL = 'postgresql://%s:%s@%s:%s/%s' % (DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME)
PSYCOPG_RAW_CON = "dbname=%s user=%s password=%s host=%s port=%s" % (DB_NAME, DB_USER, DB_PASS, DB_HOST, DB_PORT)
PEEWEE_DB_CON = PostgresqlDatabase(DB_NAME, **{'host': DB_HOST, 'password': DB_PASS, 'user': DB_USER, 'port':DB_PORT})