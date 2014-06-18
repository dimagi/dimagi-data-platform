'''
Created on Jun 17, 2014

@author: mel
'''
import psycopg2

from dimagi_data_platform import config


class StandardTableUpdater(object):
    '''
    updates a single standard table from one or more incoming data tables produced by the importers
    '''
    
    def __enter__(self):
        self.conn = psycopg2.connect(config.PSYCOPG_RAW_CON)
        self.cur = self.conn.cursor()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        self.conn.close()
    
    def update_table(self):
        pass