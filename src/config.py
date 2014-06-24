'''
Created on Jun 6, 2014

@author: mel
'''

from peewee import PostgresqlDatabase
DB_USER = 'my_user'
DB_PASS = 'my_pass'
DB_NAME = 'my_db'

CC_USER = 'ccuser@dimagi.com'

DOMAINS = ['my-test-project']

TMP_FILES_DIR = '../tmp_files'
OUTPUT_DIR = '../output'

REPORTS = ['mobile_user_monthly_lifetime_tables'] 

AWS_S3_OUTPUT_URL = 's3://mybucket/output'


from local_config import *

# THIS IS RIDICULOUS. PICK ONE.
SQLALCHEMY_DB_URL = 'postgresql://%s:%s@localhost/%s' % (DB_USER,DB_PASS, DB_NAME)
PSYCOPG_RAW_CON = "dbname=%s user=%s password=%s host=localhost" % (DB_NAME, DB_USER,DB_PASS)
PEEWEE_DB_CON = PostgresqlDatabase(DB_NAME, **{'host': 'localhost', 'password': DB_PASS, 'user': DB_USER})