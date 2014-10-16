'''
Created on Jun 8, 2014

@author: mel
'''
import datetime
import getpass
import logging
import logging.handlers
import os

from commcare_export.commcare_hq_client import CommCareHqClient

from dimagi_data_platform import incoming_data_tables, data_warehouse_db, conf
from dimagi_data_platform.data_warehouse_db import Domain
from dimagi_data_platform.extractors import ExcelExtractor, \
    CommCareExportCaseExtractor, CommCareExportFormExtractor, \
    CommCareExportUserExtractor, CommCareExportDeviceLogExtractor, \
    CommCareExportWebUserExtractor, \
    CommCareSlumberFormDefExtractor, CommCareExportExtractor, \
    SalesforceExtractor
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingDomainAnnotation, IncomingFormAnnotation, IncomingForm, \
    IncomingCases, IncomingDeviceLog
from dimagi_data_platform.loaders import DomainLoader, \
    UserLoader, FormLoader, CasesLoader, \
    VisitLoader, FormDefLoader, WebUserLoader, DeviceLogLoader, CaseEventLoader, \
    SalesforceObjectLoader
from dimagi_data_platform.utils import get_domains


logger = logging.getLogger('dimagi_data_platform')
db = conf.PEEWEE_DB_CON
        
def setup():
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    file_handler = logging.handlers.RotatingFileHandler(os.path.join(conf.LOG_FILES_DIR,'data_platform_run.log'),
                                               maxBytes=500000000,
                                               backupCount=5,)
    file_handler.setLevel(logging.DEBUG)
    root.addHandler(file_handler)

    incoming_data_tables.create_missing_tables()
    data_warehouse_db.create_missing_tables()
    
def update_hq_admin_data():
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
        
    for table_updater in table_updaters:
        table_updater.do_load()
        
    for importer in importers:
            importer.do_cleanup()

@db.commit_on_success
def load_and_cleanup(loader, extractor):
    loader.do_load()
    
    if extractor:
        extractor.do_cleanup()
                
def update_for_domain(dname, password):
    d = Domain.get(name=dname)
    
    case_extractor = CommCareExportCaseExtractor(dname)
    form_extractor = CommCareExportFormExtractor(dname)
    user_extractor = CommCareExportUserExtractor(dname)
    webuser_extractor = CommCareExportWebUserExtractor(dname)
    devicelog_extractor = CommCareExportDeviceLogExtractor(dname)
    formdef_extractor = CommCareSlumberFormDefExtractor('v0.5', dname, conf.CC_USER, password)
    
    extracters = [case_extractor,form_extractor,user_extractor,webuser_extractor,formdef_extractor, devicelog_extractor]
    logger.info('TIMESTAMP starting commcare export for domain %s' % d.name)
    api_client = CommCareHqClient('https://www.commcarehq.org',dname,version='0.5').authenticated(conf.CC_USER, password)
    
    for extracter in extracters:
        if (isinstance(extracter, CommCareExportExtractor)):
            extracter.set_api_client(api_client)
        extracter.extract()
        
    d.last_hq_import = datetime.datetime.now()
    
    logger.info('TIMESTAMP starting standard table updates for domain %s %s' % (d.name, datetime.datetime.now()))
    # these loaders should run even if there are no new forms, cases or device logs
    user_loader = UserLoader(dname)
    load_and_cleanup(user_loader,user_extractor)
    
    formdef_loader = FormDefLoader(dname)
    load_and_cleanup(formdef_loader,formdef_extractor)
    
    webuser_loader = WebUserLoader(dname)
    load_and_cleanup(webuser_loader,webuser_extractor)
    
    cases_to_import = IncomingCases.get_unimported(dname).count()
    if (cases_to_import > 0):
        logger.info('We have %d cases to import' % cases_to_import)
        case_loader = CasesLoader(dname)
        load_and_cleanup(case_loader,case_extractor)
        
    forms_to_import = IncomingForm.get_unimported(dname).count()    
    if (forms_to_import > 0):
        logger.info('We have %d forms to import' % forms_to_import)
        form_loader = FormLoader(dname)
        load_and_cleanup(form_loader,None)
        
        caseevent_loader = CaseEventLoader(dname)
        load_and_cleanup(caseevent_loader,form_extractor)
        
        visit_loader = VisitLoader(dname)
        load_and_cleanup(visit_loader, None)
        
    device_logs_to_import = IncomingDeviceLog.get_unimported(dname).count()
    if (device_logs_to_import > 0):
        logger.info('We have %d device log entries to import' % device_logs_to_import)
        devicelog_loader = DeviceLogLoader(dname)
        load_and_cleanup(devicelog_loader,devicelog_extractor)
            
def update_for_domains(domainlist, password):
    '''
    update per-domain data for domains in domainlist, using given HQ password and username specified in config_sys.json for API calls.
    '''   
    for dname in domainlist:
        try:
            update_for_domain(dname, password)
                
        except Exception, e:
                logger.error('DID NOT FINISH IMPORT/UPDATE FOR DOMAIN %s ' % dname)
                logger.exception(e)
                
def update_from_salesforce():
    sf_extractor = SalesforceExtractor(conf.SALESFORCE_USER,conf.SALESFORCE_PASS,conf.SALESFORCE_TOKEN)
    sf_extractor.do_extract()
    
    sf_loader = SalesforceObjectLoader()
    sf_loader.do_load()
    
    sf_extractor.do_cleanup()
        
def main():
        logger.info('TIMESTAMP starting run %s' % datetime.datetime.now())
        setup()
        
        
        password = getpass.getpass()
        
        logger.info('TIMESTAMP updating hq admin data - domains, forms definitions %s' % datetime.datetime.now())
        update_hq_admin_data()
        domain_list = get_domains(conf.RUN_CONF_JSON)
        
        logger.info('TIMESTAMP starting domain updates %s' % datetime.datetime.now())
        logger.info('domains for run are: %s' % ','.join(domain_list))
        update_for_domains(domain_list, password)
        
        #update_from_salesforce()
    
if __name__ == '__main__':
    main()

