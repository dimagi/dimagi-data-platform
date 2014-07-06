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
from psycopg2.extras import LoggingConnection

from dimagi_data_platform import data_warehouse_db, incoming_data_tables, conf
from dimagi_data_platform.data_warehouse_db import Domain
from dimagi_data_platform.importers import ExcelImporter, \
    CommCareExportCaseImporter, CommCareExportFormImporter
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingDomainAnnotation, IncomingFormAnnotation
from dimagi_data_platform.standard_table_updaters import DomainTableUpdater, \
    UserTableUpdater, FormTableUpdater, CasesTableUpdater, CaseEventTableUpdater, \
    VisitTableUpdater, FormDefTableUpdater
from dimagi_data_platform.utils import get_domains


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
    for dname in domainlist:
        
        d = Domain.get(name=dname)
        since = d.last_hq_import
        d.last_hq_import = datetime.datetime.now()
        d.save()
        
        api_client = CommCareHqClient('https://www.commcarehq.org', dname).authenticated(conf.CC_USER, password)
        
        importers = []
        importers.append(CommCareExportCaseImporter(api_client, since))
        importers.append(CommCareExportFormImporter(api_client, since))
    
        for importer in importers:
            importer.do_import()
        
        with LoggingConnection(conf.PSYCOPG_RAW_CON) as dbconn:
            LoggingConnection.initialize(dbconn, logger)
            table_updaters = []
            table_updaters.append(UserTableUpdater(dbconn, dname))
            table_updaters.append(FormTableUpdater(dbconn, dname))
            table_updaters.append(CasesTableUpdater(dbconn, dname))
            table_updaters.append(CaseEventTableUpdater(dbconn, dname))
            
            for table_updater in table_updaters:
                table_updater.update_table()
        
        vt = VisitTableUpdater(dbconn, dname)
        vt.update_table()
        

def main():
        setup()
        password = getpass.getpass()
        
        update_platform_data()
        domain_list = get_domains(conf.DOMAIN_CONF_JSON)
        
        logger.info('domains for run are: %s' % ','.join(domain_list))
        
        run_for_domains(domain_list, password)
        
        r_script_path = os.path.abspath('R/')
        conf_path = os.path.abspath('.')
        
        for report in conf.REPORTS:
            run_proccess_and_log('Rscript', [os.path.join(r_script_path, '%s.R' % report), r_script_path, conf_path, ','.join(["'%s'" % n for n in domain_list])])
        
        run_proccess_and_log('aws', ['s3', 'sync', conf.OUTPUT_DIR, conf.AWS_S3_OUTPUT_URL])
    
if __name__ == '__main__':
    main()

