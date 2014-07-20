'''
Created on Jun 8, 2014

@author: mel
'''
import datetime
import getpass
import logging
import os
import subprocess

from commcare_export.commcare_hq_client import CommCareHqClient

from dimagi_data_platform import incoming_data_tables, data_warehouse_db, conf
from dimagi_data_platform.data_warehouse_db import Domain
from dimagi_data_platform.importers import ExcelImporter, \
    CommCareExportCaseImporter, CommCareExportFormImporter
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingDomainAnnotation, IncomingFormAnnotation, IncomingForm, \
    IncomingCases
from dimagi_data_platform.standard_table_updaters import DomainTableUpdater, \
    UserTableUpdater, FormTableUpdater, CasesTableUpdater, CaseEventTableUpdater, \
    VisitTableUpdater, FormDefTableUpdater
from dimagi_data_platform.utils import get_domains, configure_logger


logger = logging.getLogger('dimagi_data_platform')

def run_proccess_and_log(cmd, args_list):
    proc_list = [cmd] + args_list
    proc = subprocess.Popen(proc_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if stdout:
        logger.info(stdout)
    if stderr:
        logger.error(stderr)
        
def setup():
    configure_logger(logger)
    incoming_data_tables.create_missing_tables()
    data_warehouse_db.create_missing_tables()
    
def update_platform_data():
    '''
    update from master lists of domains, forms etc
    '''
    importers = []
    importers.append(ExcelImporter(IncomingDomain, "domains.xlsx"))
    importers.append(ExcelImporter(IncomingDomainAnnotation, "domain_annotations.xlsx"))
    importers.append(ExcelImporter(IncomingFormAnnotation, "form_annotations.xlsx"))
    
    for importer in importers:
        importer.do_import()
    
    table_updaters = []
    table_updaters.append(DomainTableUpdater())
    table_updaters.append(FormDefTableUpdater())
        
    for table_updater in table_updaters:
        table_updater.update_table()
        
    for importer in importers:
            importer.do_cleanup()
            
def run_for_domains(domainlist, password):
    
    for dname in domainlist:
        try:
            d = Domain.get(name=dname)
            since = d.last_hq_import
            
            importers = []
            importers.append(CommCareExportCaseImporter(since, dname))
            importers.append(CommCareExportFormImporter(since, dname))
            
            
    
            logger.info('TIMESTAMP starting commcare export for domain %s %s' % (d.name, datetime.datetime.now()))
            
            api_client = CommCareHqClient('https://www.commcarehq.org', dname).authenticated(conf.CC_USER, password)
            
            for importer in importers:
                importer.set_api_client(api_client)
                importer.do_import()
            
            d.last_hq_import = datetime.datetime.now()
            d.save()
            
            forms_to_import = IncomingForm.get_unimported(dname).count()
            cases_to_import = IncomingCases.get_unimported(dname).count()
            
            if (forms_to_import > 0) or (cases_to_import >0):
                logger.info('We have %d forms and %d cases to import' % (forms_to_import, cases_to_import))
                table_updaters = []
                table_updaters.append(UserTableUpdater(dname))
                table_updaters.append(FormTableUpdater(dname))
                table_updaters.append(CasesTableUpdater(dname))
                table_updaters.append(CaseEventTableUpdater(dname))
                table_updaters.append(VisitTableUpdater(dname))
                
                logger.info('TIMESTAMP starting standard table updates for domain %s %s' % (d.name, datetime.datetime.now()))
                for table_updater in table_updaters:
                    table_updater.update_table()
                    
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
        run_for_domains(domain_list, password)
    
if __name__ == '__main__':
    main()

