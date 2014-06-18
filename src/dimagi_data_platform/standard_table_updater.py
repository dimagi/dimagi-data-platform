'''
Created on Jun 17, 2014

@author: mel
'''
class StandardTableUpdater(object):
    '''
    updates a single standard table from one or more incoming data tables produced by the importers
    '''
    
    def __init__(self, dbconn):
        self.conn=dbconn
    
    def update_table(self):
        pass