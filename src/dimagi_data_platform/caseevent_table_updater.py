'''
Created on Jun 17, 2014

@author: mel
'''


from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class CaseEventTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = domain
        super(CaseEventTableUpdater, self).__init__(dbconn)
        
    def update_table(self):
        with self.conn.cursor() as curs:
            curs.execute("delete from case_event where form_id in (select form_id from form where domain like '%s');"%self.domain)
            curs.execute("insert into case_event(form_id, case_id) (select form.form_id, cases.case_id from incoming_form, form, cases "
                         "where incoming_form.form_id = form.form_id and incoming_form.case_id = cases.case_id "
                         "and incoming_form.domain like '%s');" %self.domain)
        
