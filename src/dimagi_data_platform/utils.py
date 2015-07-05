'''
Created on Jul 1, 2014

@author: mel
'''
import logging
import subprocess
import xlrd
import os

from dimagi_data_platform.data_warehouse_db import Domain
from commcare_export.commcare_hq_client import CommCareHqClient

logger = logging.getLogger(__name__)

def get_domains(run_conf_json):
    '''
    returns names of domains to run on, specified by names or filters.
    named domains are always included in a run.
    filters are AND'd together - a domain is included only if it matches all filters
    there is one special case. if the value of domains is the string "all", all domains are included.
    the active_only flag is applied after the names section is processed, and indicates whether inactive domains should be returned
    '''
    active_only = run_conf_json['active_domains_only']
    domain_conf_json = run_conf_json['domains']
    logger.info('processing domain conf sections: %s'% domain_conf_json)
    
    if domain_conf_json == 'all':
        return [dm.name for dm in Domain.select().where(Domain.active==True)] if active_only else [dm.name for dm in Domain.select()]
    
    conf_domains = []
    if 'names' in domain_conf_json:
        named_domains = domain_conf_json['names']
        conf_domains.extend(named_domains)
    
    if active_only:
        active_domains = Domain.select().where((Domain.name << conf_domains) & (Domain.active == True))
        return [d.name for d in active_domains]
    return conf_domains

def get_domains_with_forms(username, password):
    """Returns a list of all domains with atleast one form submission from the Admin Domain Report"""
    report_url = "https://www.commcarehq.org/hq/admin/export/real_project_spaces/"

    # we only need the client for the authenticated session, so we authenticate ourselves against a test domain 
    api_client = CommCareHqClient('https://www.commcarehq.org',"test",version='0.5').authenticated(username, password)
    auth = api_client._CommCareHqClient__auth  # we need to pull the auth attribute directly
    response = api_client.session.get(report_url, params={"es_is_test": "false"}, auth=auth)

    TMP_FILENAME = '_tmp_HQ_DOMAIN_REPORT_.xls'
    output = open(TMP_FILENAME, 'wb')
    output.write(response.content)
    output.close()

    PROJECT_COL = 0
    FORM_COL = 11

    workbook = xlrd.open_workbook(TMP_FILENAME)
    sheet = workbook.sheet_by_index(0)
    domains_by_forms = dict(zip(sheet.col_values(PROJECT_COL)[1:], sheet.col_values(FORM_COL)[1:]))
    domains_by_forms = dict([(d, float(nf)) for (d, nf) in domains_by_forms.iteritems()])

    os.remove(TMP_FILENAME)

    domain_list = []
    for domain, nforms in domains_by_forms.iteritems():
        if nforms > 0:
            domain_list.append(domain)
    return domain_list


def break_into_chunks(l, n):
    '''
    take a list and return a list of lists of length n
    used for bulk inserts
    '''
    if n < 1:
        n = 1
    return [l[i:i + n] for i in range(0, len(l), n)]

def run_proccess_and_log(cmd, args_list):
    proc_list = [cmd] + args_list
    proc = subprocess.Popen(proc_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if stdout:
        logger.info(stdout)
    if stderr:
        logger.error(stderr)
        
def dict_flatten(d, delim='.', prefix=[]):
    def pairs(d, prefix):
        def flatten(k, v):
            if isinstance(v, dict):
                return pairs(v, k)
            else:
                return [(k, v)]
        for k, v in d.iteritems():
            for e in flatten(prefix + [k], v):
                yield e
    return dict((delim.join(k), v) for k, v in pairs(d, prefix))

def dict_str_vals(d):
    def str_vals(d):
        for k, v in d.iteritems():
            if v is None:
                yield (k,v)
            elif type(v) is list:
                quoted_list_str = ','.join(['"%s"'% val for val in v])
                yield (k,quoted_list_str)
            else:
                yield (k,'%s'%v)
    return dict((k, v) for k, v in str_vals(dict_flatten(d)) if v is not None)

