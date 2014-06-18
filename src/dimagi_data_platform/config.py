'''
Created on Jun 6, 2014

@author: mel
'''
from peewee import PostgresqlDatabase


DB_USER = 'importer'
DB_PASS = 'notthis'
DB_NAME = 'data_platform_db_2'

# PICK ONE
SQLALCHEMY_DB_URL = 'postgresql://%s:%s@localhost/%s' % (DB_USER,DB_PASS, DB_NAME)
PSYCOPG_RAW_CON = "dbname=%s user=%s password=%s host=localhost" % (DB_NAME, DB_USER,DB_PASS)
PEEWEE_DB_CON = PostgresqlDatabase(DB_NAME, **{'host': 'localhost', 'password': DB_PASS, 'user': DB_USER})

CC_USER = 'mloudon@dimagi.com'

DOMAINS = ['melissa-test-project',]
#DOMAINS = ['aaharbaseline',] # 6 minutes
#DOMAINS = ['crs-imci',] # 8 minutes
#DOMAINS = ['crs-remind',] # 


DATA_DIR = 'data/'