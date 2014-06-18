'''
Created on Jun 17, 2014

@author: mel
'''


from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class FormTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = domain
        super(FormTableUpdater, self).__init__(dbconn)
        
    def update_table(self):
        with self.conn.cursor() as curs:
            curs.execute("delete from form where domain like '%s'" % self.domain)

            curs.execute("insert into form (form_id, xmlns, app_id, time_start, time_end, user_id, domain) "
                "(select form_id, xmlns, app_id, to_timestamp(replace(time_start,'T',' '),'YYYY-MM-DD HH24:MI:SS') "
                "as time_start, to_timestamp(replace(time_end,'T',' '),'YYYY-MM-DD HH24:MI:SS') as time_end, users.id as user_id, incoming_form.domain "
                "from incoming_form, users where incoming_form.domain like '%s' and users.user_id = incoming_form.user_id "
                "group  by form_id, xmlns, app_id, time_start,time_end, users.id, incoming_form.domain);"  % self.domain)

        
        
