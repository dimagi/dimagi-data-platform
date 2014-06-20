'''
Created on Jun 6, 2014

@author: mel
'''
import logging

from commcare_export.commcare_minilinq import CommCareHqEnv
from commcare_export.env import BuiltInEnv, JsonPathEnv
from commcare_export.writers import SqlTableWriter, CsvTableWriter

from dimagi_data_platform import importer
from dimagi_data_platform.pg_copy_writer import CsvPlainWriter, PgCopyWriter


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
        
        writer = PgCopyWriter(self.engine.connect(),self.api_client.project)
          
        env = BuiltInEnv() | CommCareHqEnv(self.api_client) | JsonPathEnv({})
        result = self.query.eval(env)
        
        if len(list(env.emitted_tables())) > 0:
            with writer:
                for table in env.emitted_tables():
                   
                    writer.write_table(table)
              
        else:
            pass
