'''
Created on Jun 6, 2014

@author: mel
'''
from commcare_export.minilinq import Emit, Literal, Map, Apply, Reference, List
from sqlalchemy.orm import query

from dimagi_data_platform import commcare_export_importer
from dimagi_data_platform.incoming_data_tables import IncomingCases


class CommCareExportCaseImporter(commcare_export_importer.CommCareExportImporter):
    '''
    An importer for cases
    '''
    _hstore_col_name = None
    _incoming_table_class = IncomingCases
    
    def __init__(self, api_client):
        '''
        Constructor
        '''
        self.api_client = api_client      
        super(CommCareExportCaseImporter,self).__init__(self.api_client)
    
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