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

from dimagi_data_platform.form_table_updater import FormTableUpdater
from dimagi_data_platform.user_table_updater import UserTableUpdater
from dimagi_data_platform.visit_table_updater import VisitTableUpdater


logger = logging.getLogger(__name__)
hdlr = logging.FileHandler('/var/tmp/data_platform_run.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)

def run_proccess_and_log(cmd,args_list):
    proc_list = [cmd]+ args_list
    proc=subprocess.Popen(proc_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if stdout:
        logger.info(stdout)
    if stderr:
        logger.error(stderr)
        
def setup():
    incoming_data_tables.create_missing_tables()
    data_warehouse_db.create_missing_tables()

def main():
        setup()
        password = getpass.getpass()

        for domain in conf.DOMAINS:
            api_client = CommCareHqClient('https://www.commcarehq.org',domain).authenticated(conf.CC_USER,password )
            
            importers = []
            importers.append(CommCareExportCaseImporter(api_client))
            importers.append(CommCareExportFormImporter(api_client))
        
            for importer in importers:
                importer.do_import()
            
            
            with LoggingConnection(conf.PSYCOPG_RAW_CON) as dbconn:
                LoggingConnection.initialize(dbconn,logger)
                table_updaters = []
                table_updaters.append(UserTableUpdater(dbconn,domain))
                table_updaters.append(FormTableUpdater(dbconn,domain))
                table_updaters.append(CasesTableUpdater(dbconn,domain))
                table_updaters.append(CaseEventTableUpdater(dbconn,domain))
                
                for table_updater in table_updaters:
                    table_updater.update_table()
            
            vt = VisitTableUpdater(dbconn,domain)
            vt.update_table()
        
        r_script_path = os.path.abspath('R/')
        conf_path = os.path.abspath('.')
        
        for report in conf.REPORTS:
            run_proccess_and_log('Rscript',[os.path.join(r_script_path,'%s.R' % report), r_script_path, conf_path])
        
        run_proccess_and_log('aws',['s3','sync',conf.OUTPUT_DIR,conf.AWS_S3_OUTPUT_URL])
    
if __name__ == '__main__':
    main()

