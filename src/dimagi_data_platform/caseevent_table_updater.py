'''
Created on Jun 17, 2014

@author: mel
'''


from dimagi_data_platform.data_warehouse_db import Domain
from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class CaseEventTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        super(CaseEventTableUpdater, self).__init__(dbconn)
        
    def update_table(self):
        with self.conn.cursor() as curs:
            curs.execute("delete from case_event where form_id in (select id from form where domain_id = '%d');"%self.domain.id)
            curs.execute("insert into case_event(form_id, case_id) (select form.id, cases.id from incoming_form, form, cases, domain "
                         "where incoming_form.form_id = form.form_id and incoming_form.case_id = cases.case_id "
                         " and domain.id = form.domain_id "
                         " and domain.id = cases.domain_id "
                         " and domain.id = '%d');" %self.domain.id)
        
