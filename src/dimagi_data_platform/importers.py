'''
Created on Jun 6, 2014

@author: mel
'''
import logging
import os

from commcare_export.commcare_minilinq import CommCareHqEnv
from commcare_export.env import BuiltInEnv, JsonPathEnv
from commcare_export.minilinq import Emit, Literal, Map, FlatMap, Reference, \
    Apply, List
from pandas.io.excel import ExcelFile
from playhouse.postgres_ext import HStoreField
import sqlalchemy

import conf
from dimagi_data_platform.incoming_data_tables import IncomingForm, \
    IncomingCases
from dimagi_data_platform.pg_copy_writer import PgCopyWriter


logger = logging.getLogger(__name__)

class Importer(object):
    '''
    Knows how to read data from a source and write to incoming data tables.
    '''
    _incoming_table_class = None

    def __init__(self, _incoming_table_class):
        '''
        Constructor
        '''
        self.engine = sqlalchemy.create_engine(conf.SQLALCHEMY_DB_URL)
        
    @property
    def _get_table_name(self):
        return self._incoming_table_class._meta.db_table
    
    @property   
    def _get_field_names(self):
        return [v for v in self._incoming_table_class._meta.fields.keys]
    
    @property   
    def _get_db_cols(self):
        return [v.db_column for v in self._incoming_table_class._meta.fields.values()]
    
    @property   
    def _get_attribute_db_cols(self):
        '''
        returns the names of all columns except the hstores
        '''
        cols = [v.db_column for k, v in self._incoming_table_class._meta.fields.iteritems() if not isinstance(v,HStoreField)]
        return cols
    
    @property       
    def _get_hstore_db_col(self):
        '''
        returns the name of the first hstore col found in the table.
        there should only be one - it's just a place to dump attributes we don't have a column for
        '''
        hstorecols = [v.db_column for k, v in self._incoming_table_class._meta.fields.iteritems() if isinstance(v,HStoreField)]
        if hstorecols:
            return hstorecols[0]
        
    def do_import(self):
        pass

class CommCareExportImporter(Importer):
    '''
    An importer that uses commcare-export
    '''

    def __init__(self, incoming_table_class, api_client, since):
        '''
        Constructor
        '''
        self.api_client = api_client
        self.since = since
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
        
        env = BuiltInEnv() | CommCareHqEnv(self.api_client, self.since) | JsonPathEnv({})
        result = self._get_query.eval(env)
        
        if (self._get_table_name in [t['name'] for t in env.emitted_tables()]):
            with writer:
                for table in env.emitted_tables():
                    if table['name'] == self._get_table_name:
                        writer.write_table(table, self._get_attribute_db_cols, self._get_hstore_db_col)
              
        else:
            logger.warn('no table emitted with name %s' % self._get_table_name)
            
class CommCareExportFormImporter(CommCareExportImporter):
    '''
    An importer for cases
    '''

    _incoming_table_class = IncomingForm
    
    def __init__(self, api_client, since):
        '''
        Constructor
        '''
        super(CommCareExportFormImporter, self).__init__(self._incoming_table_class,api_client,since)
    
    @property
    def _get_query(self):
        # headings need to be lower case and not reserved words for the postgresql copy to work
        form_query = Emit(table=self._get_table_name,
                    headings=[Literal('form_id'),
                              Literal('xmlns'),
                              Literal('app_id'),
                              Literal('domain'),
                              Literal('app_version'),
                              Literal('device_id'),
                              Literal('user_id'),
                              Literal('is_phone_submission'),
                              Literal('time_start'),
                              Literal('time_end'),
                              Literal('received_on'),
                              Literal('case_id'),
                              Literal('alt_case_id'),
                              Literal('created'),
                              Literal('updated'),
                              Literal('closed')],
                    source=Map(source=FlatMap(body=Reference('form..case'),
                                              source=Apply(Reference('api_data'), Literal('form'))),
                               body=List([Reference('id'),
                              Reference('$.form.@xmlns'),
                              Reference('$.app_id'),
                              Reference('$.domain'),
                              Reference('$.metadata.appVersion'),
                              Reference('$.metadata.deviceID'),
                              Reference('$.metadata.userID'),
                              Reference('$.is_phone_submission'),
                              Reference('$.metadata.timeStart'),
                              Reference('$.metadata.timeEnd'),
                              Reference('$.received_on'),
                              Reference('@case_id'),
                              Reference('case_id'),
                              Apply(Reference('bool'), Reference('create')),
                              Apply(Reference('bool'), Reference('update')),
                              Apply(Reference('bool'), Reference('close')), ])))
        return form_query
    
class CommCareExportCaseImporter(CommCareExportImporter):
    '''
    An importer for cases
    '''

    _incoming_table_class = IncomingCases
    
    def __init__(self, api_client, since):
        '''
        Constructor
        '''  
        super(CommCareExportCaseImporter,self).__init__( self._incoming_table_class, api_client,since)
    
    @property
    def _get_query(self):
        case_query = Emit(table=self._get_table_name, 
                   headings=[Literal('api_id'),
                             Literal('case_id'),
                             Literal('closed'),
                             Literal('date_closed'),
                             Literal('date_modified'),
                             Literal('domain'),
                             Literal('user_id'),
                             Literal('date_opened'),
                             Literal('case_type'),
                             Literal('owner_id'),
                             Literal('parent_id')],
                   source=Map(source=Apply(Reference('api_data'),Literal('case')),
                              body=List([Reference('id'),
                             Reference('case_id'),
                             Reference('closed'),
                             Reference('date_closed'),
                             Reference('date_modified'),
                             Reference('domain'),
                             Reference('user_id'),
                             Reference('properties.date_opened'),
                             Reference('properties.case_type'),
                             Reference('properties.owner_id'),
                             Reference('indices.parent.case_id')])))
        return case_query

class ExcelImporter(Importer):
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


    