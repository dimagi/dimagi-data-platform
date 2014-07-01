'''
Created on Jun 17, 2014

@author: mel
'''


from dimagi_data_platform.data_warehouse_db import Domain
from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class UserTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        super(UserTableUpdater, self).__init__(dbconn)
        
    def update_table(self):
        with self.conn.cursor() as curs:
            curs.execute("delete from users where domain_id = '%d'" % self.domain.id)

            curs.execute("insert into users(user_id,domain_id) "
                             "(select user_id, domain.id from incoming_cases, domain where domain.name like incoming_cases.domain "
                             " and domain.id ='%d' union select user_id, domain.id "
                             "from incoming_cases,domain where domain.name like incoming_cases.domain and domain.id='%d' union "
                             "select owner_id, domain.id from incoming_cases, domain "
                             "where domain.name like incoming_cases.domain and domain.id='%d');" % (self.domain.id,self.domain.id,self.domain.id))

        
        
