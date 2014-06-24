'''
Created on Jun 6, 2014

@author: mel
'''
import logging

from commcare_export.commcare_minilinq import CommCareHqEnv
from commcare_export.env import BuiltInEnv, JsonPathEnv

from dimagi_data_platform import importer
from dimagi_data_platform.pg_copy_writer import PgCopyWriter


logger = logging.getLogger(__name__)
hdlr = logging.FileHandler('/var/tmp/data_platform_run.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

class CommCareExportImporter(importer.Importer):
    '''
    An importer that uses commcare-export
    '''

    def __init__(self, api_client, query, incoming_table_name, db_cols, hstore_col_name):
        '''
        Constructor
        '''
        self.api_client = api_client
        self.query = query
        self.db_cols = db_cols
        self.hstore_col_name = hstore_col_name
        self.incoming_table_name = incoming_table_name
        
        super(CommCareExportImporter, self).__init__()
    
    def do_import(self):
        
        if not self.api_client:
            raise Exception('CommCareExportImporter needs an initialized API client')
        
        if not self.engine:
            raise Exception('CommCareExportImporter needs a database connection engine')
        
        writer = PgCopyWriter(self.engine.connect(), self.api_client.project)
          
        env = BuiltInEnv() | CommCareHqEnv(self.api_client) | JsonPathEnv({})
        result = self.query.eval(env)
        
        if (self.incoming_table_name in [t['name'] for t in env.emitted_tables()]):
            with writer:
                for table in env.emitted_tables():
                    if table['name'] == self.incoming_table_name:
                        writer.write_table(table, self.db_cols, self.hstore_col_name)
              
        else:
            logger.warn('no table emitted with name %s' % self.incoming_table_name)
