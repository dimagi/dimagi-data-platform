'''
Created on Jun 8, 2014

@author: mel
'''
import getpass

from commcare_export.commcare_hq_client import CommCareHqClient

from dimagi_data_platform import config
from dimagi_data_platform.caseevent_table_updater import CaseEventTableUpdater
from dimagi_data_platform.cases_table_updater import CasesTableUpdater
from dimagi_data_platform.commcare_export_case_importer import CommCareExportCaseImporter
from dimagi_data_platform.commcare_export_form_importer import CommCareExportFormImporter
from dimagi_data_platform.data_warehouse_db import drop_and_create
from dimagi_data_platform.form_table_updater import FormTableUpdater
from dimagi_data_platform.user_table_updater import UserTableUpdater
from dimagi_data_platform.visit_table_updater import VisitTableUpdater


def main():
    
    for domain in config.DOMAINS:
        api_client = CommCareHqClient('https://www.commcarehq.org',domain).authenticated(config.CC_USER, getpass.getpass())
        
        importers = []
        importers.append(CommCareExportCaseImporter(api_client))
        importers.append(CommCareExportFormImporter(api_client))
    
        for importer in importers:
            importer.do_import()
        
        drop_and_create()
        table_updaters = []
        table_updaters.append(UserTableUpdater(domain))
        table_updaters.append(FormTableUpdater(domain))
        table_updaters.append(CasesTableUpdater(domain))
        table_updaters.append(CaseEventTableUpdater(domain))
        table_updaters.append(VisitTableUpdater(domain))
        
        for table_updater in table_updaters:
            with table_updater as t:
                t.update_table()
            
    
if __name__ == '__main__':
    main()

