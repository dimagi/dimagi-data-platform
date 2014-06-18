'''
Created on Jun 17, 2014

@author: mel
'''


from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class UserTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, domains):
        '''
        Constructor
        '''
        self.domains = domains
        
    def update_table(self):
        
        self.cur.execute('insert into users(user_id,domain) (select user_id, domain from incoming_cases union select user_id, domain from incoming_form union select owner_id, domain from incoming_cases);')
        self.conn.commit()
        
        
