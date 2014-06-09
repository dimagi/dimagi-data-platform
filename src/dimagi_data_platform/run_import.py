'''
Created on Jun 8, 2014

@author: mel
'''
import getpass

from commcare_export.commcare_hq_client import CommCareHqClient

from dimagi_data_platform import config
from dimagi_data_platform.commcare_export_case_importer import CommCareExportCaseImporter
from dimagi_data_platform.commcare_export_form_importer import CommCareExportFormImporter


def main():
    
    for domain in config.DOMAINS:
        api_client = CommCareHqClient('https://www.commcarehq.org',domain).authenticated(config.CC_USER, getpass.getpass())
        
        importers = []
        importers.append(CommCareExportCaseImporter(api_client))
        #importers.append(CommCareExportFormImporter(api_client))
    
        for importer in importers:
            importer.do_import()
    
if __name__ == '__main__':
    main()

