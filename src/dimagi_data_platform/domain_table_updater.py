'''
Created on Jun 17, 2014

@author: mel
'''
from dimagi_data_platform.data_warehouse_db import Domain, Sector, DomainSector
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingDomainAnnotation
from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class DomainTableUpdater(StandardTableUpdater):
    '''
    updates the domain table, plus sectors and subsectors
    '''
    
    _first_col_names_to_skip = ['Total','Mean','STD']
    
    def __init__(self, dbconn):
        super(DomainTableUpdater, self).__init__(dbconn)
    
    def update_table(self):
        
        for row in IncomingDomain.select():
            attrs = row.attributes

            dname = attrs['Project']
            
            if dname not in self._first_col_names_to_skip:
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
            
        for row in IncomingDomainAnnotation.select():
            
            attrs = row.attributes
            dname = attrs['Domain name']
            business_unit = attrs['Business unit']
            sector_names = [k.replace('Sector_', '') for k, v in attrs.iteritems() if (k.startswith('Sector_') & (v == 'Yes'))]
            
            try:
                domain = Domain.get(name=dname)
                domain.business_unit = business_unit
                if domain.attributes:
                    domain.attributes = domain.attributes.update(attrs)
                else:
                    domain.attributes = attrs
            
                for secname in sector_names:
                    try:
                        sec = Sector.get(name=secname)
                    except Sector.DoesNotExist:
                        sec = Sector(name=secname)
                        sec.save()
                    ds = DomainSector(domain=domain, sector=sec)
                    ds.save()
            except Domain.DoesNotExist:
                logger.warn('Domain referenced i domain annotations table with name %s, does not exist' % (dname))

            

                