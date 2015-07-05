'''
Created on Jun 8, 2014

@author: mel
'''
import datetime
import getpass
import logging
import logging.handlers
import os
import time
import json

from commcare_export.commcare_hq_client import CommCareHqClient

from dimagi_data_platform import incoming_data_tables, data_warehouse_db, conf, emails
from dimagi_data_platform.data_warehouse_db import Domain, HQExtractLog
from dimagi_data_platform.extractors import ExcelExtractor, \
    CommCareExportCaseExtractor, CommCareExportFormExtractor, \
    CommCareExportUserExtractor, CommCareExportDeviceLogExtractor, \
    CommCareExportWebUserExtractor, \
    CommCareSlumberFormDefExtractor, CommCareExportExtractor, \
    SalesforceExtractor, HQAdminAPIExtractor, WebuserAdminAPIExtractor, \
    ProjectSpaceAdminAPIExtractor
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingDomainAnnotation, IncomingFormAnnotation, IncomingForm, \
    IncomingCases, IncomingDeviceLog
from dimagi_data_platform.loaders import DomainLoader, \
    UserLoader, FormLoader, CasesLoader, \
    VisitLoader, FormDefLoader, WebUserLoader, DeviceLogLoader, CaseEventLoader, \
    SalesforceObjectLoader, ApplicationLoader
from dimagi_data_platform.utils import get_domains, get_domains_with_forms


logger = logging.getLogger('dimagi_data_platform')
db = conf.PEEWEE_DB_CON
        
def setup():
    logger.setLevel(logging.DEBUG)
    
    # console logger
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    
    # debug logger
    file_handler = logging.handlers.RotatingFileHandler(os.path.join(conf.LOG_FILES_DIR,'data_platform_run.log'),
                                               maxBytes=500000000,
                                               backupCount=5,)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)
    
    # warning and error logger
    warning_handler = logging.handlers.RotatingFileHandler(os.path.join(conf.LOG_FILES_DIR,'data_platform_warning.log'),
                                               maxBytes=5000000,
                                               backupCount=5,)
    warning_handler.setLevel(logging.WARNING)
    warning_handler.setFormatter(formatter)
    root.addHandler(warning_handler)

    incoming_data_tables.create_missing_tables()
    data_warehouse_db.create_missing_tables()
    
def update_hq_admin_data(username, password):
    '''
    update domains, form definitions, and anything else that is not extracted per-domain from APIs
    '''
    starting_time = time.clock()
    webuser_extractor = WebuserAdminAPIExtractor(username,password)
    domain_extractor = ProjectSpaceAdminAPIExtractor(username,password)
    domain_annotation_extractor = ExcelExtractor(IncomingDomainAnnotation, "domain_annotations.xlsx")
    form_annotation_extractor = ExcelExtractor(IncomingFormAnnotation, "form_annotations.xlsx")
    extractors = [domain_extractor,webuser_extractor, domain_annotation_extractor,form_annotation_extractor]
    timing_dict = {}
    for extractor in extractors:
        timing = run_extract(extractor)
        extractor_name = extractor.__class__.__name__
        logger.info("admin %s extract took %s seconds" % (extractor_name, timing))
        timing_dict[extractor_name] = timing
    
    domain_loader = DomainLoader()
    timing = load_and_cleanup(domain_loader,domain_extractor, domain_annotation_extractor)
    timing_dict[domain_loader.__class__.__name__] = timing
    
    webuser_loader = WebUserLoader()
    timing = load_and_cleanup(webuser_loader,webuser_extractor)
    timing_dict[webuser_loader.__class__.__name__] = timing

    logger.info("Update Admin Data took %s seconds" % (time.clock() - starting_time))
    logger.info("%s" % timing_dict)

    
@db.commit_on_success
def load_and_cleanup(loader, *extractors):
    starting_time = time.clock()  
    loader.do_load()
    
    for extractor in extractors:
        extractor.do_cleanup()
    duration = time.clock() - starting_time
    return duration

def run_extract(extractor):
    start_time = time.clock()
    extractor.do_extract()
    duration = time.clock() - start_time
    return duration
                
def update_for_domain(dname, username, password, incremental):
    d = Domain.get(name=dname)
    
    case_extractor = CommCareExportCaseExtractor(dname, incremental)
    form_extractor = CommCareExportFormExtractor(dname, incremental)
    user_extractor = CommCareExportUserExtractor(dname)
    archived_user_extractor = CommCareExportUserExtractor(dname, archived = True)
    webuser_extractor = CommCareExportWebUserExtractor(dname)
    devicelog_extractor = CommCareExportDeviceLogExtractor(dname)
    formdef_extractor = CommCareSlumberFormDefExtractor('v0.5', dname, conf.CC_USER, password)
    
    extractors = [case_extractor,form_extractor,user_extractor,archived_user_extractor,webuser_extractor,formdef_extractor, devicelog_extractor]
    logger.info('TIMESTAMP starting commcare export for domain %s' % d.name)
    api_client = CommCareHqClient('https://www.commcarehq.org',dname,version='0.5').authenticated(username, password)
    
    timing_dict = {}
    for extractor in extractors:
        if (isinstance(extractor, CommCareExportExtractor)):
            extractor.set_api_client(api_client)
        timing = run_extract(extractor)
        extractor_name = extractor.__class__.__name__
        logger.info("%s extract for domain %s took %s seconds" % (extractor_name, dname, timing))
        timing_dict[extractor_name] = timing
    
    logger.info('TIMESTAMP starting standard table updates for domain %s %s' % (d.name, datetime.datetime.now()))
    # these loaders should run even if there are no new forms, cases or device logs
    user_loader = UserLoader(dname,api_version='v0.5',username=conf.CC_USER, password=password)
    timing = load_and_cleanup(user_loader,user_extractor, user_extractor)
    timing_dict[user_loader.__class__.__name__] = timing

    app_loader = ApplicationLoader(dname)
    timing = load_and_cleanup(app_loader) # don't clean up yet, formdef_loader uses the same incoming table
    timing_dict[app_loader.__class__.__name__] = timing

    formdef_loader = FormDefLoader(dname)
    timing = load_and_cleanup(formdef_loader,formdef_extractor)
    timing_dict[formdef_loader.__class__.__name__] = timing
    
    webuser_loader = WebUserLoader(dname)
    timing = load_and_cleanup(webuser_loader,webuser_extractor)
    timing_dict[webuser_loader.__class__.__name__] = timing
    
    cases_to_import = IncomingCases.get_unimported(dname).count()
    if (cases_to_import > 0):
        logger.info('We have %d cases to import' % cases_to_import)
        case_loader = CasesLoader(dname, user_loader)
        timing = load_and_cleanup(case_loader,case_extractor)
        timing_dict[case_loader.__class__.__name__] = timing
        
    forms_to_import = IncomingForm.get_unimported(dname).count()    
    if (forms_to_import > 0):
        logger.info('We have %d forms to import' % forms_to_import)
        form_loader = FormLoader(dname, user_loader)
        timing = load_and_cleanup(form_loader)
        timing_dict[form_loader.__class__.__name__] = timing
        
        caseevent_loader = CaseEventLoader(dname)
        timing = load_and_cleanup(caseevent_loader,form_extractor)
        timing_dict[caseevent_loader.__class__.__name__] = timing
    
    visit_loader = VisitLoader(dname, regenerate_all=(not incremental))
    timing = load_and_cleanup(visit_loader)
    timing_dict[visit_loader.__class__.__name__] = timing
        
    device_logs_to_import = IncomingDeviceLog.get_unimported(dname).count()
    if (device_logs_to_import > 0):
        logger.info('We have %d device log entries to import' % device_logs_to_import)
        devicelog_loader = DeviceLogLoader(dname, user_loader)
        timing = load_and_cleanup(devicelog_loader,devicelog_extractor)
        timing_dict[devicelog_loader.__class__.__name__] = timing

    logger.info("--- Timing Log for domain: %s" % dname)
    logger.info("%s" % timing_dict)
    return timing_dict

def get_missed_extractions(dname, start_time):
    d = Domain.get(name=dname)
    extractor_names = ["CommCareExportCaseExtractor", "CommCareExportFormExtractor", "CommCareExportUserExtractor",
        "CommCareExportWebUserExtractor", "CommCareExportDeviceLogExtractor", "CommCareSlumberFormDefExtractor"]
    missed_extractions = []
    for e in extractor_names:
        try:
            last_extract_log = HQExtractLog.get_last_extract_log(e, d)
            if last_extract_log.extract_end < start_time:
                missed_extractions.append(e)
        except HQExtractLog.DoesNotExist:
            missed_extractions.append(e)
    return missed_extractions

import os
import errno

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def update_for_domains(domainlist, username, password, start_time, incremental = True, allow_rerun=True):
    '''
    update per-domain data for domains in domainlist, using given HQ password and username specified in config_sys.json for API calls.
    '''
    missed_extractions= {}
    timing_dict = {}
    timing_logs = {}
    for dname in domainlist:
        starting_time = time.clock()
        try:
            timing_log = update_for_domain(dname, username, password, incremental)
            timing_logs[dname] = timing_log
                
        except Exception, e:
                logger.error('DID NOT FINISH IMPORT/UPDATE FOR DOMAIN %s ' % dname)
                logger.exception(e)
        timing_dict[dname] = time.clock() - starting_time
        logger.info("Update for %s took %s seconds" % (dname, timing_dict[dname]))
        missed_extractions[dname] = get_missed_extractions(dname, start_time)

    missed_extractions = dict((k,v) for k,v in missed_extractions.items() if v)
    logger.info("Missed Extractions: %s" % missed_extractions)
    if allow_rerun:
        failed_domain_list = [dname for dname, missed in missed_extractions.items() if "CommCareExportFormExtractor" in missed]
        logger.info("Rerunning data pull for the following domains: %s" % failed_domain_list)
        update_for_domains(failed_domain_list, username, password, start_time, incremental=incremental, allow_rerun=False)
    
        logger.info("---- Timing Info ----")
        logger.info("%s" % timing_dict)
        logger.info("---- Timing Logs ----")
        logger.info("%s" % timing_logs)

    time_str = "%s" % datetime.datetime.now()
    if not allow_rerun:
        time_str = "rerun-" + time_str
    make_sure_path_exists('timing')
    with open('timing/timing_info-%s' % time_str, 'w') as f:
        json.dump(timing_dict, f)
    with open('timing/timing_logs-%s' % time_str, 'w') as f:
        json.dump(timing_logs, f)
    return timing_dict, timing_log, missed_extractions  
                
def update_from_salesforce():
    starting_time = time.clock()
    sf_extractor = SalesforceExtractor(conf.SALESFORCE_USER,conf.SALESFORCE_PASS,conf.SALESFORCE_TOKEN)
    sf_extractor.do_extract()
    
    sf_loader = SalesforceObjectLoader()
    sf_loader.do_load()
    
    sf_extractor.do_cleanup()
    logger.info("Salesforce Update took: %s seconds" % time.clock() - starting_time)

        
def main():
        start_time = datetime.datetime.now()
        logger.info('TIMESTAMP starting run %s' % start_time)
        setup()
        
        username = conf.CC_USER
        password = getpass.getpass("Please enter your CommCareHQ password: ")

        if conf.EMAIL_FROM_USER:
            gmail_pwd = getpass.getpass("Please enter your the email password for %s: " % conf.EMAIL_FROM_USER)

        # default to incremental update
        incremental = conf.RUN_CONF_JSON['incremental'] if 'incremental' in conf.RUN_CONF_JSON else True
        
        logger.info('TIMESTAMP updating hq admin data - domains, forms definitions %s' % datetime.datetime.now())
        emails.send_initial_email(gmail_pwd, start_time, incremental)
        update_hq_admin_data(username, password)
        domain_list = get_domains(conf.RUN_CONF_JSON)

        domains_with_forms = get_domains_with_forms(username, password)
        domain_list = [d for d in domain_list if d in domains_with_forms]
        
        logger.info('TIMESTAMP starting domain updates %s' % datetime.datetime.now())
        logger.info('domains for run are: %s' % ','.join(domain_list))

        emails.send_intermediary_email(gmail_pwd, domain_list)

        timing_dict, _, missed_extractions = update_for_domains(domain_list, username, password,
                                                                start_time, incremental = incremental)

        emails.send_finish_email(gmail_pwd, domain_list, timing_dict, missed_extractions)
        
        update_from_salesforce()
    
if __name__ == '__main__':
    main()

