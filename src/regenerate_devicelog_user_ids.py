import datetime
import getpass
import logging
import logging.handlers
import os

from commcare_export.commcare_hq_client import CommCareHqClient

from dimagi_data_platform import incoming_data_tables, data_warehouse_db, conf
from dimagi_data_platform.data_warehouse_db import Domain, DeviceLog
from dimagi_data_platform.loaders import UserLoader, set_devicelog_users
from dimagi_data_platform.utils import get_domains

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

        
def main():
        setup()        
        username = conf.CC_USER
        password = getpass.getpass()

        domain_list = get_domains(conf.RUN_CONF_JSON)
        logger.info('TIMESTAMP starting device log user id regen %s' % datetime.datetime.now())
        logger.info('domains for run are: %s' % ','.join(domain_list))

        for dname in domain_list:
            logger.info('Regenerating user_id for logs in %s' % dname)
            d = Domain.get(name=dname)
            user_loader = UserLoader(dname,api_version='v0.5',username=conf.CC_USER, password=password)
            xform_ids = set([l.form for l in DeviceLog.select(DeviceLog.form).where(DeviceLog.domain == d.id)])
            for xf_id in xform_ids:
                logger.info('Regenerating user_id for logs in xform: %s' % xf_id)
                set_devicelog_users(xf_id, user_loader)
    
if __name__ == '__main__':
    main()