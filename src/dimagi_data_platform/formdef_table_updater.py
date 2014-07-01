'''
Created on Jun 17, 2014

@author: mel
'''
import logging

from dimagi_data_platform.data_warehouse_db import Domain, FormDefinition, \
    Subsector, FormDefinitionSubsector, Sector
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingFormAnnotation
from dimagi_data_platform.standard_table_updater import StandardTableUpdater


logger = logging.getLogger(__name__)

class FormDefTableUpdater(StandardTableUpdater):
    '''
    updates the form definition table, plus subsectors
    '''
    
    def __init__(self, dbconn):
        super(FormDefTableUpdater, self).__init__(dbconn)
    
    def update_table(self):
        
        for row in IncomingFormAnnotation.select():
            attrs = row.attributes
            xmlns = attrs['Form xmlns']
            app_id = attrs['Application ID']
            dname = attrs['Domain name']
            subsector_names = [k.replace('Subsector_', '') for k, v in attrs.iteritems() if (k.startswith('Subsector_') & (v == 'Yes'))]
            
            try:
                domain = Domain.get(name=dname)
                
                try:
                    fd = FormDefinition.get(xmlns=xmlns, app_id=app_id, domain=domain)
                except FormDefinition.DoesNotExist:
                    fd = FormDefinition(xmlns=xmlns, app_id=app_id, domain=domain)
                
                fd.attributes = attrs
                fd.save()
                
                for sname in subsector_names:
                    try:
                        sub = Subsector.get(name=sname)
                    except Subsector.DoesNotExist:
                        sub = Subsector(name=sname)
                        sub.save()
                    fs = FormDefinitionSubsector(fd, sub)
                    fs.save()
                    
            except Domain.DoesNotExist:
                logger.warn('Domain with name %s does not exist, could not add Form Definition with xmlns %s and app ID %s' % (dname, xmlns, app_id))
