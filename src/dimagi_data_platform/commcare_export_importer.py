'''
Created on Jun 6, 2014

@author: mel
'''

import logging
import os
import zipfile

from commcare_export.commcare_minilinq import CommCareHqEnv
from commcare_export.env import BuiltInEnv, JsonPathEnv
from commcare_export.writers import SqlTableWriter, CsvTableWriter

from dimagi_data_platform import importer
from dimagi_data_platform.pg_copy_writer import CsvPlainWriter


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
        
        writer = CsvPlainWriter(self.api_client.project)
          
        env = BuiltInEnv() | CommCareHqEnv(self.api_client) | JsonPathEnv({})
        result = self.query.eval(env)
        
        if len(list(env.emitted_tables())) > 0:
            with writer:
                for table in env.emitted_tables():
                    logger.debug('Writing %s', table['name'])
                    writer.write_table(table)
              
        else:
            logger.warn('Nothing emitted')
        
        
        conn = self.engine.connect()
        self.engine.echo = True
        
        for table in env.emitted_tables():
            abspath = ''
            with open('%s.csv' % table['name'],'r') as csv_file:
                abspath =  os.path.abspath(csv_file.name)
            
            colnames = table['headings']
            copy_sql = "COPY %s FROM '%s' WITH CSV HEADER" % (table['name'],abspath)
            logger.debug('copying table %s into db with sql %s'%(table['name'],copy_sql))
            
            trans = conn.begin()
            conn.execute(copy_sql)
            trans.commit()
            
            conn.close()
            
        
    def write_to_db(self):
        pass
        
