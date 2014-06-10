'''
Created on Jun 6, 2014

@author: mel
'''
from commcare_export.minilinq import Emit, Literal, Map, Apply, Reference, List, \
    FlatMap

from dimagi_data_platform import commcare_export_importer


class CommCareExportFormImporter(commcare_export_importer.CommCareExportImporter):
    '''
    An importer for cases
    '''
    
    def __init__(self, api_client):
        '''
        Constructor
        '''
        self.api_client = api_client
        
        super(CommCareExportFormImporter, self).__init__(self.api_client, self.get_query)
    
    @property
    def get_query(self):
        # headings need to be lower case and not reserved words for the postgresql copy to work
        form_query = Emit(table='incoming_form',
                    headings=[Literal('form_id'),
                              Literal('xmlns'),
                              Literal('app_id'),
                              Literal('domain'),
                              Literal('app_version'),
                              Literal('device_id'),
                              Literal('user_id'),
                              Literal('is_phone_submission'),
                              Literal('time_dtart'),
                              Literal('time_end'),
                              Literal('received_on'),
                              Literal('case_id'),
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
                              Apply(Reference('bool'), Reference('create')),
                              Apply(Reference('bool'), Reference('update')),
                              Apply(Reference('bool'), Reference('close')), ])))
        return form_query
        
        
    def write_to_db(self):
        pass
