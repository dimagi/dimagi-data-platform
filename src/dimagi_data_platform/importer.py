'''
Created on Jun 6, 2014

@author: mel
'''
import sqlalchemy

import config


class Importer(object):
    '''
    Knows how to read data from a source and write to the data plaform standard tables
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.engine = sqlalchemy.create_engine(config.SQLALCHEMY_DB_URL)
    
    def do_import(self):
        pass
    