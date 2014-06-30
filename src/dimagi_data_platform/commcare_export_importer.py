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

    def __init__(self, incoming_table_class, api_client):
        '''
        Constructor
        '''
        self.api_client = api_client
        self._incoming_table_class = incoming_table_class
        
        super(CommCareExportImporter, self).__init__(self._incoming_table_class)
    
    @property
    def _get_query(self):
        pass
    
    def do_import(self):
        
        if not self.api_client:
            raise Exception('CommCareExportImporter needs an initialized API client')
        
        if not self.engine:
            raise Exception('CommCareExportImporter needs a database connection engine')
        
        writer = PgCopyWriter(self.engine.connect(), self.api_client.project)
          
        env = BuiltInEnv() | CommCareHqEnv(self.api_client) | JsonPathEnv({})
        result = self._get_query.eval(env)
        
        if (self._get_table_name in [t['name'] for t in env.emitted_tables()]):
            with writer:
                for table in env.emitted_tables():
                    if table['name'] == self._get_table_name:
                        writer.write_table(table, self._get_attribute_db_cols, self._get_hstore_db_col)
              
        else:
            logger.warn('no table emitted with name %s' % self._get_table_name)
