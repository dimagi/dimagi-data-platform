'''
Created on Jun 6, 2014

@author: mel
'''

import logging

from commcare_export.commcare_minilinq import CommCareHqEnv
from commcare_export.env import BuiltInEnv, JsonPathEnv
from commcare_export.writers import SqlTableWriter

from dimagi_data_platform import importer


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')


class CommCareExportImporter(importer.Importer):
    '''
    An importer that uses commcare-export
    '''

    def __init__(self, api_client, query):
        '''
        Constructor
        '''
        self.api_client = api_client
        self.query = query
        
        super(CommCareExportImporter,self).__init__()
        
    def do_import(self):
        
        if not self.api_client:
            raise Exception('CommCareExportImporter needs an initialized API client')
        
        if not self.engine:
            raise Exception('CommCareExportImporter needs a database connection engine')
        
        
        
        writer = SqlTableWriter(self.engine.connect())
      
        env = BuiltInEnv() | CommCareHqEnv(self.api_client) | JsonPathEnv({})
        result = self.query.eval(env)
        
        if len(list(env.emitted_tables())) > 0:
            with writer:
                for table in env.emitted_tables():
                    logger.debug('Writing %s', table['name'])
                    writer.write_table(table)
        else:
            logger.warn('Nothing emitted')
        
    def write_to_db(self):
        pass
        
