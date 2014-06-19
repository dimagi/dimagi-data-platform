'''
Created on Jun 8, 2014

@author: mel
'''
import getpass
import logging

from commcare_export.commcare_hq_client import CommCareHqClient
from psycopg2.extras import LoggingConnection

from dimagi_data_platform import config
from dimagi_data_platform.caseevent_table_updater import CaseEventTableUpdater
from dimagi_data_platform.cases_table_updater import CasesTableUpdater
from dimagi_data_platform.commcare_export_case_importer import CommCareExportCaseImporter
from dimagi_data_platform.commcare_export_form_importer import CommCareExportFormImporter
from dimagi_data_platform.data_warehouse_db import create_missing_tables
from dimagi_data_platform.form_table_updater import FormTableUpdater
from dimagi_data_platform.user_table_updater import UserTableUpdater
from dimagi_data_platform.visit_table_updater import VisitTableUpdater


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')

def main():
    
        password = getpass.getpass()

        create_missing_tables()
        
        for domain in config.DOMAINS:
            api_client = CommCareHqClient('https://www.commcarehq.org',domain).authenticated(config.CC_USER,password )
            
            importers = []
            importers.append(CommCareExportCaseImporter(api_client))
            importers.append(CommCareExportFormImporter(api_client))
        
            for importer in importers:
                importer.do_import()
            
            
            
            with LoggingConnection(config.PSYCOPG_RAW_CON) as dbconn:
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
            
    
if __name__ == '__main__':
    main()

