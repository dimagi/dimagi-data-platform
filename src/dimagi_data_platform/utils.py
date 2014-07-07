'''
Created on Jul 1, 2014

@author: mel
'''
import logging

from dimagi_data_platform.data_warehouse_db import Domain


logger = logging.getLogger(__name__)

def configure_logger(lg):
    logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='/var/tmp/data_platform_run.log',
                    filemode='w')
    
    logger_consol_handler = logging.StreamHandler()
    logger_consol_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger_consol_handler.setFormatter(formatter)

    logging.getLogger('').addHandler(logger_consol_handler)

def get_domains(domain_conf_json):
    '''
    returns names of domains to run on, specified by names or filters.
    named domains are always included in a run.
    filters are AND'd together - a domain is included only if it matches all filters
    '''
    logger.info('processing domain conf sections: %s'% domain_conf_json)
    
    if domain_conf_json == 'all':
        return [dm.name for dm in Domain.select()]
    
    domains = []
    domain_db_cols = [d.db_column for d in Domain._meta.fields.values()]
    
    if 'names' in domain_conf_json:
        named_domains = [d['name'] for d in domain_conf_json['names']]
        domains.extend(named_domains)
        
    if 'filters' in domain_conf_json:
        filters = domain_conf_json['filters']
        filter_lists = []
        
        for filter in filters:
            filter_by = filter['filter_by']
            values = filter['values']
            values_list = [v.strip() for v in values.split(',')]
            
            filter_domains = []
            
            if (filter_by in domain_db_cols):
                db_col_filter = Domain.raw('select name from domain where %s in (%s)' % (filter_by, ','.join(["'%s'"%v for v in values_list])))
                filter_domains = [d.name for d in db_col_filter]
            
            elif filter_by == 'subsector':
                all_domains = Domain.select()
                for domain in all_domains:
                    sub_names = [ds.subsector.name for ds in domain.domainsubsectors]
                    if len(set(sub_names) & set(values_list)) > 0:
                        filter_domains.append(domain.name)
            
            elif filter_by == 'sector':
                all_domains = Domain.select()
                for domain in all_domains:
                    sec_names = [ds.sector.name for ds in domain.domainsectors]
                    if len(set(sec_names) & set(values_list)) > 0:
                        filter_domains.append(domain.name)
            else:
                for val in values_list:
                    val_domains = Domain.select().where(Domain.attributes.contains({filter_by: val}))
                    filter_domains.extend([v.name for v in val_domains])
            
            filter_lists.append(filter_domains)
        if filter_lists:
            domains.extend(set(filter_lists[0]).intersection(*filter_lists))
    
    return list(set(domains))
