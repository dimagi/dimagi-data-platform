'''
Created on Jun 17, 2014

@author: mel
'''


from dimagi_data_platform.data_warehouse_db import Domain
from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class CasesTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        super(CasesTableUpdater, self).__init__(dbconn)
        
    def update_table(self):
        with self.conn.cursor() as curs:
            curs.execute("delete from cases where domain_id = '%d'" % self.domain.id)
            curs.execute("insert into cases (case_id, user_id, owner_id, parent_id, case_type, date_opened, date_modified, closed, date_closed, domain_id) "
                             "(select distinct on (case_id) case_id, a.id as user_id, b.id as owner_id, parent_id, case_type, date_opened, date_modified, closed, date_closed, domain.id "
                             "from incoming_cases, users as a, users as b, domain "
                             "where a.user_id = incoming_cases.user_id and b.user_id = incoming_cases.owner_id and a.domain_id = domain.id and b.domain_id = domain.id "
                             "and domain.name = incoming_cases.domain "
                             "and domain.id = '%d' "
                             "order by case_id, date_modified desc);" % (self.domain.id))
        
        
