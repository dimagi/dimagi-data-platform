from datetime import datetime
import logging.handlers
import os

from dimagi_data_platform import conf
from dimagi_data_platform.data_warehouse_db import HQExtractLog, Domain
from dimagi_data_platform.loaders import VisitLoader


logger = logging.getLogger('dimagi_data_platform')

def setup():
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    file_handler = logging.handlers.RotatingFileHandler(os.path.join(conf.LOG_FILES_DIR,'data_platform_run.log'),
                                               maxBytes=500000000,
                                               backupCount=5,)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

def main():
        setup()
        
        extracted_domain_ids = HQExtractLog.select(HQExtractLog.domain)
        domain_list = Domain.select().where((Domain.id << extracted_domain_ids)&(Domain.name == 'tulasalud'))
        
        logger.info('TIMESTAMP starting domain updates %s' % datetime.now())
        logger.info('domains for run are: %s' % ','.join([d.name for d in domain_list]))
        
        for domain in domain_list:
                visit_loader = VisitLoader(domain.name)
                visit_loader.do_load()
    
if __name__ == '__main__':
    main()