'''
Created on Jun 17, 2014

@author: mel
'''
from dimagi_data_platform.data_warehouse_db import Domain
from dimagi_data_platform.incoming_data_tables import IncomingDomain
from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class DomainTableUpdater(StandardTableUpdater):
    '''
    updates the domain table, plus sectors and subsectors
    '''
    
    def __init__(self, dbconn):
        super(DomainTableUpdater, self).__init__(dbconn)
    
    def update_table(self):
        
        for row in IncomingDomain.select():
            attrs = row.attributes
            dname = attrs['Project']
            
            try:
                domain = Domain.get(name=dname)
            except Domain.DoesNotExist:
                domain = Domain.create(name=dname)
            
            domain.organization = attrs['Organization']
            domain.country = attrs['Deployment Country']
            domain.services = attrs['Services']
            domain.project_state = attrs['Project State']
            domain.attributes = attrs
            
            domain.save()