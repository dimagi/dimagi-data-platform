'''
Created on Jun 6, 2014

@author: mel
'''

DB_USER = 'importer'
DB_PASS = 'notthis'
DB_NAME = 'data_platform_db'
DB_URL = 'postgresql://%s:%s@/%s' % (DB_USER,DB_PASS,DB_NAME)