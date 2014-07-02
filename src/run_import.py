'''
Created on Jun 8, 2014

@author: mel
'''
import getpass
import logging
import os
import subprocess

from commcare_export.commcare_hq_client import CommCareHqClient
from psycopg2.extras import LoggingConnection

from dimagi_data_platform import data_warehouse_db, incoming_data_tables, conf
from dimagi_data_platform.caseevent_table_updater import CaseEventTableUpdater
from dimagi_data_platform.cases_table_updater import CasesTableUpdater
from dimagi_data_platform.commcare_export_case_importer import CommCareExportCaseImporter
from dimagi_data_platform.commcare_export_form_importer import CommCareExportFormImporter
from dimagi_data_platform.domain_table_updater import DomainTableUpdater
from dimagi_data_platform.excel_importer import ExcelImporter
from dimagi_data_platform.form_table_updater import FormTableUpdater
from dimagi_data_platform.formdef_table_updater import FormDefTableUpdater
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingDomainAnnotation, IncomingFormAnnotation
from dimagi_data_platform.user_table_updater import UserTableUpdater
from dimagi_data_platform.utils import get_domains
from dimagi_data_platform.visit_table_updater import VisitTableUpdater


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
    
    with LoggingConnection(conf.PSYCOPG_RAW_CON) as dbconn:
        LoggingConnection.initialize(dbconn, logger)
        table_updaters = []
        table_updaters.append(DomainTableUpdater(dbconn))
        table_updaters.append(FormDefTableUpdater(dbconn))
        
        for table_updater in table_updaters:
            table_updater.update_table()
            
def run_for_domains(domainlist, password):
    for domain in domainlist:
        api_client = CommCareHqClient('https://www.commcarehq.org', domain).authenticated(conf.CC_USER, password)
        
        importers = []
        importers.append(CommCareExportCaseImporter(api_client))
        importers.append(CommCareExportFormImporter(api_client))
    
        for importer in importers:
            importer.do_import()
        
        with LoggingConnection(conf.PSYCOPG_RAW_CON) as dbconn:
            LoggingConnection.initialize(dbconn, logger)
            table_updaters = []
            table_updaters.append(UserTableUpdater(dbconn, domain))
            table_updaters.append(FormTableUpdater(dbconn, domain))
            table_updaters.append(CasesTableUpdater(dbconn, domain))
            table_updaters.append(CaseEventTableUpdater(dbconn, domain))
            
            for table_updater in table_updaters:
                table_updater.update_table()
        
        vt = VisitTableUpdater(dbconn, domain)
        vt.update_table()

def main():
        setup()
        password = getpass.getpass()
        
        update_platform_data()
        domain_list = get_domains(conf.DOMAIN_CONF_JSON)
        print 'domains for run are: %s' % ','.join(domain_list)
        logger.info('domains for run are: %s' % ','.join(domain_list))
        
        run_for_domains(domain_list, password)
        
        r_script_path = os.path.abspath('R/')
        conf_path = os.path.abspath('.')
        
        for report in conf.REPORTS:
            run_proccess_and_log('Rscript', [os.path.join(r_script_path, '%s.R' % report), r_script_path, conf_path, ','.join(["'%s'" % n for n in domain_list])])
        
        run_proccess_and_log('aws', ['s3', 'sync', conf.OUTPUT_DIR, conf.AWS_S3_OUTPUT_URL])
    
if __name__ == '__main__':
    main()

