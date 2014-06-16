'''
Created on Jun 6, 2014

@author: mel
'''

DB_USER = 'importer'
DB_PASS = 'notthis'
DB_NAME = 'data_platform_db_2'
DB_URL = 'postgresql://%s:%s@localhost/%s' % (DB_USER,DB_PASS,DB_NAME)

CC_USER = 'mloudon@dimagi.com'

DOMAINS = ['melissa-test-project',]
#DOMAINS = ['aaharbaseline',] # 6 minutes
#DOMAINS = ['crs-imci',] # 8 minutes
#DOMAINS = ['crs-remind',] # 


DATA_DIR = 'data/'