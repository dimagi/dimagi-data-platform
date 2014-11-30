'''
Created on Jul 1, 2014

@author: mel
'''
import logging
import subprocess

from dimagi_data_platform.data_warehouse_db import Domain

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

