'''
Created on Jun 17, 2014

@author: mel
'''


from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class UserTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = domain
        super(UserTableUpdater, self).__init__(dbconn)
        
    def update_table(self):
        with self.conn.cursor() as curs:
            curs.execute("delete from users where domain like '%s'" % self.domain)

            curs.execute("insert into users(user_id,domain) "
                             "(select user_id, domain from incoming_cases where domain like '%s' union select user_id, domain "
                             "from incoming_form where domain like '%s' union "
                             "select owner_id, domain from incoming_cases where domain like '%s');" % (self.domain,self.domain,self.domain))

        
        
