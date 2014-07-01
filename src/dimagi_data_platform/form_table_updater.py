'''
Created on Jun 17, 2014

@author: mel
'''


from dimagi_data_platform.data_warehouse_db import Domain
from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class FormTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        super(FormTableUpdater, self).__init__(dbconn)
        
    def update_table(self):
        with self.conn.cursor() as curs:
            curs.execute("delete from form where domain_id = '%d'" % self.domain.id)

            curs.execute("insert into form (form_id, xmlns, app_id, time_start, time_end, user_id, domain_id) "
                "(select distinct on (form_id) form_id, xmlns, app_id, to_timestamp(replace(time_start,'T',' '),'YYYY-MM-DD HH24:MI:SS') "
                "as time_start, to_timestamp(replace(time_end,'T',' '),'YYYY-MM-DD HH24:MI:SS') as time_end, users.id as user_id, domain.id "
                "from incoming_form, users, domain "
                "where incoming_form.domain like domain.name "
                "and domain.id='%d' and users.user_id = incoming_form.user_id  and users.domain_id = domain.id and domain.name = incoming_form.domain "
                "order by form_id, time_start);"  % self.domain.id)

        
        
