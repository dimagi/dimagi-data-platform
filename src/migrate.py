from playhouse.migrate import *

from dimagi_data_platform import conf, incoming_data_tables, data_warehouse_db
from dimagi_data_platform.data_warehouse_db import Domain, HQExtractLog
from dimagi_data_platform.extractors import CommCareExportFormExtractor, \
    CommCareExportCaseExtractor


def drop_last_import_col():
    migrator = PostgresqlMigrator(conf.PEEWEE_DB_CON)
    migrate(migrator.drop_column('domain', 'last_hq_import'),)
    
def set_last_extract():
    extracted_domains = Domain.select().where(~(Domain.last_hq_import >> None))
    
    for domain in extracted_domains:
        form_extractor = CommCareExportFormExtractor(domain.name)
        case_extractor = CommCareExportCaseExtractor(domain.name)
        form_log = HQExtractLog(extractor = form_extractor.__class__.__name__,domain = domain, extract_start = domain.last_hq_import, extract_end = domain.last_hq_import)
        case_log = HQExtractLog(extractor = case_extractor.__class__.__name__,domain = domain, extract_start = domain.last_hq_import, extract_end = domain.last_hq_import)
        form_log.save()
        case_log.save()

def main ():
    data_warehouse_db.create_missing_tables()
    set_last_extract()
    #drop_last_import_col()

if __name__ == '__main__':
    main()