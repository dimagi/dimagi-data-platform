'''
Created on Jun 8, 2014

@author: mel
'''
import datetime
import getpass
import logging

from commcare_export.commcare_hq_client import CommCareHqClient

from dimagi_data_platform import incoming_data_tables, data_warehouse_db, conf
from dimagi_data_platform.data_warehouse_db import Domain
from dimagi_data_platform.extractors import ExcelExtractor, \
    CommCareExportCaseExtractor, CommCareExportFormExtractor, \
    CommCareExportUserExtractor, CommCareExportDeviceLogExtractor, \
    CommCareExportWebUserExtractor
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingDomainAnnotation, IncomingFormAnnotation, IncomingForm, \
    IncomingCases
from dimagi_data_platform.loaders import DomainLoader, \
    UserLoader, FormLoader, CasesLoader, CaseEventLoader, \
    VisitLoader, FormDefLoader
from dimagi_data_platform.utils import get_domains, configure_logger


logger = logging.getLogger('dimagi_data_platform')
        
def setup():
    configure_logger(logger)
    incoming_data_tables.create_missing_tables()
    data_warehouse_db.create_missing_tables()
    
def update_platform_data():
    '''
    update domains, form definitions, and anything else that is not extracted per-domain from APIs
    '''
    importers = []
    importers.append(ExcelExtractor(IncomingDomain, "domains.xlsx"))
    importers.append(ExcelExtractor(IncomingDomainAnnotation, "domain_annotations.xlsx"))
    importers.append(ExcelExtractor(IncomingFormAnnotation, "form_annotations.xlsx"))
    
    for importer in importers:
        importer.do_extract()
    
    table_updaters = []
    table_updaters.append(DomainLoader())
    table_updaters.append(FormDefLoader())
        
    for table_updater in table_updaters:
        table_updater.do_load()
        
    for importer in importers:
            importer.do_cleanup()
            
def update_for_domains(domainlist, password):
    '''
    update per-domain data for domains in domainlist, using given HQ password and username specified in config_sys.json for API calls.
    '''   
    for dname in domainlist:
        try:
            d = Domain.get(name=dname)
            since = d.last_hq_import
            
            importers = []
            importers.append(CommCareExportCaseExtractor(since, dname))
            importers.append(CommCareExportFormExtractor(since, dname))
            importers.append(CommCareExportUserExtractor(since, dname))
            importers.append(CommCareExportWebUserExtractor(since, dname))

            logger.info('TIMESTAMP starting commcare export for domain %s %s' % (d.name, datetime.datetime.now()))
            api_client = CommCareHqClient('https://www.commcarehq.org',dname,version='0.5').authenticated(conf.CC_USER, password)
            
            for importer in importers:
                importer.set_api_client(api_client)
                importer.do_extract()
            
            d.last_hq_import = datetime.datetime.now()
            d.save()
            
            forms_to_import = IncomingForm.get_unimported(dname).count()
            cases_to_import = IncomingCases.get_unimported(dname).count()
            
            if (forms_to_import > 0) or (cases_to_import > 0):
                logger.info('We have %d forms and %d cases to import' % (forms_to_import, cases_to_import))
                loaders = []
                loaders.append(UserLoader(dname))
                loaders.append(FormLoader(dname))
                loaders.append(CasesLoader(dname))
                loaders.append(CaseEventLoader(dname))
                loaders.append(VisitLoader(dname))
                
                logger.info('TIMESTAMP starting standard table updates for domain %s %s' % (d.name, datetime.datetime.now()))
                for table_updater in loaders:
                    table_updater.do_load()
                    
                for importer in importers:
                    importer.do_cleanup()
            else:
                logger.info('No forms or cases to import for domain %s' % (d.name))
                
        except Exception, e:
                logger.error('DID NOT FINISH IMPORT/UPDATE FOR DOMAIN %s ' % d.name)
                logger.exception(e)

        
def main():
        logger.info('TIMESTAMP starting run %s' % datetime.datetime.now())
        setup()
        password = getpass.getpass()
        
        logger.info('TIMESTAMP updating platform data - domains, forms definitions %s' % datetime.datetime.now())
        update_platform_data()
        domain_list = get_domains(conf.DOMAIN_CONF_JSON)
        
        logger.info('TIMESTAMP starting domain updates %s' % datetime.datetime.now())
        logger.info('domains for run are: %s' % ','.join(domain_list))
        update_for_domains(domain_list, password)
    
if __name__ == '__main__':
    main()

