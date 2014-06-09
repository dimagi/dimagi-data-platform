'''
Created on Jun 8, 2014

@author: mel
'''
import getpass

from commcare_export.commcare_hq_client import CommCareHqClient

from dimagi_data_platform import config
from dimagi_data_platform.commcare_export_case_importer import CommCareExportCaseImporter


def main():

    project = "melissa-test-project"

    api_client = CommCareHqClient('https://www.commcarehq.org',project).authenticated(config.CC_USER, getpass.getpass())
    
    case_importer = CommCareExportCaseImporter(api_client)

    case_importer.do_import()
    
if __name__ == '__main__':
    main()

