'''
Created on Jun 17, 2014

@author: mel
'''


from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class CaseEventTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, domain):
        '''
        Constructor
        '''
        self.domain = domain
        
    def update_table(self):
        self.cur.execute("insert into case_event(form_id, case_id) (select form.form_id, cases.case_id from incoming_form, form, cases "
                         "where incoming_form.form_id = form.form_id and incoming_form.case_id = cases.case_id);")
        self.conn.commit()
        
        
