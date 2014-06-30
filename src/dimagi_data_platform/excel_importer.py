'''
Created on Jun 28, 2014

@author: mel
'''
import logging
import os

from pandas.io.excel import ExcelFile

from dimagi_data_platform import importer, conf


logger = logging.getLogger(__name__)
hdlr = logging.FileHandler('/var/tmp/data_platform_run.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

class ExcelImporter(importer.Importer):
    '''
    An importer for excel files. 
    One sheet only for now. 
    Expects column names in first row, rest of rows mapped 1:1 to incoming table rows.
    Unique identifier (or unique for domain) in first column.
    '''
    
    def __init__(self, incoming_table_class, file_name):
        '''
        Constructor
        '''
        self._incoming_table_class = incoming_table_class
        self.file_name = file_name
        
        self.workbook = ExcelFile(os.path.join(conf.INPUT_DIR, file_name))
        
        super(ExcelImporter, self).__init__(self._incoming_table_class)
        
    def _get_workbook_rowdicts(self):
        '''
        returns list of key-value dicts from keys in first row
        '''
        return self.workbook.parse().to_dict(outtype='records')
        
    def _get_workbook_keys(self):
        '''
        returns list of key-value dicts from keys in first row
        '''
        return self.workbook.parse().to_dict().keys()
        
    
    def do_import(self):
        
        dq = self._incoming_table_class.delete()
        dq.execute()
        
        db_col_keys = [k for k in self._get_workbook_keys() if k in self._get_db_cols]
        hstore_keys = [h for h in self._get_workbook_keys() if h not in self._get_db_cols]
        
        for row in self._get_workbook_rowdicts():
            db_col_dict = dict((k,v) for k,v in row.iteritems() if k in db_col_keys)
            hstore_col_dict = dict((k,unicode(v)) for k,v in row.iteritems() if k in hstore_keys)
            
            insert_dict = db_col_dict
            insert_dict[self._get_hstore_db_col] = hstore_col_dict

            self._incoming_table_class.create(**insert_dict)

            
            
        
        