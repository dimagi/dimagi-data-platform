'''
Created on Jun 17, 2014

@author: mel
'''


from peewee import prefetch

from dimagi_data_platform.data_warehouse_db import User, Form, CaseEvent, Cases, \
    Visit, Interaction, FormVisit
from dimagi_data_platform.standard_table_updater import StandardTableUpdater

home_visit_forms = set(['http://openrosa.org/formdesigner/E41E21BB-32F9-435B-A3E9-BEB08C4743A1',
'http://openrosa.org/formdesigner/0FA17225-70BC-4284-A38B-6C4E16E6CA4F',
'http://openrosa.org/formdesigner/4A49479F-BEBD-498D-A08B-4F0EAAD4DDBB',
'http://openrosa.org/formdesigner/FDD31334-4885-4C31-AEA4-59547CCC5C9E',
'http://openrosa.org/formdesigner/93BC80C0-E9EC-4A0E-BDE0-ACE69CB30B88',
'http://openrosa.org/formdesigner/63866D7C-42FC-43DD-8EFA-E02C74729DD6',
'http://openrosa.org/formdesigner/5314838B-00ED-4556-A656-100EAFD0603F',
'http://openrosa.org/formdesigner/EF241BF0-6230-4390-A8BE-31803E7A135E',
'http://openrosa.org/formdesigner/a6fafcf2cc3e280533aa0325b513d73f3c41c4ef',
'http://openrosa.org/formdesigner/AC501474-0BA9-4915-9A8C-A8A8F6C57BFF',
'http://openrosa.org/formdesigner/EA8FB6FC-E269-440F-993E-AD07F733BF31',
'http://openrosa.org/formdesigner/fa8deed6f9b7c955a077e987a919e2815cf2afa5',
'http://openrosa.org/formdesigner/36c07ca15eb402fb6ba3cedd709c6ce7e1e83685'])


class VisitTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = domain
        super(VisitTableUpdater, self).__init__(dbconn)
        
    def create_visit(self, user, visited_forms, visited_cases):
        
        v = Visit.create(user=user)
        
        for cs in visited_cases:
            i = Interaction.create(case=cs, visit=v)
            
        v.home_visit = v.home_visit if v.home_visit else False
        
        v.form_duration = 0
        for fp in visited_forms:
            v.form_duration = v.form_duration + (fp.time_end - fp.time_start).total_seconds()
            fv = FormVisit.create(visit=v, form=fp)
            
            if fp.xmlns in home_visit_forms:
                v.home_visit = True
        
        v.time_start = min(visited_forms, key=lambda x : x.time_start).time_start
        v.time_end = max(visited_forms, key=lambda x : x.time_end).time_end
     
        v.save()
        print('saved visit %d with %d cases and %d forms') % (v.visit, len(visited_cases), len(visited_forms))
        
    def update_table(self):
        
        users = User.select().where(User.domain == self.domain).order_by(User.user)

        delete_query = Visit.delete().where(Visit.user << users)
        delete_query.execute()
        
        forms = Form.select().where(~(Form.time_end >> None) & ~(Form.time_start >> None)).order_by(Form.time_start)
        ces = CaseEvent.select().join(Cases)
        
        users_prefetch = prefetch(users, forms, ces)
        
        for u in users_prefetch:
            print("GETTING VISITS FOR USER %s" % u.user)
            prev_visited_forms = []
            prev_visited_cases = []
            
            for frm in u.forms_prefetch:
                
                case_events = frm.caseevents_prefetch
                if len(case_events) > 0:
                    form_cases = [cec.case for cec in case_events]
                    prev_visited_case_ids = [pvc.case for pvc in prev_visited_cases]
                    
                    # if there is overlap between cases already in this visit and cases in this form don't save yet, but add the form cases to the visit cases
                    if len(set([fc.case for fc in form_cases]) & set(prev_visited_case_ids)) > 0:
                        for frm_case in form_cases:
                            if frm_case.case not in prev_visited_case_ids:
                                prev_visited_cases.append(frm_case)
                        
                        prev_visited_forms.append(frm)
                    
                    # if there is overlap between this form's related cases and cases already in this visit, or cases in this form and cases related to cases already in this visit
                    elif (len(set(prev_visited_case_ids) & set([fc.parent for fc in form_cases if fc.parent])) > 0) | (len(set([fc.case for fc in form_cases]) & set([pvc.parent for pvc in prev_visited_cases if pvc.parent])) > 0):
                        for frm_case in form_cases:
                            if frm_case.case not in prev_visited_case_ids:
                                prev_visited_cases.append(frm_case)
                        prev_visited_forms.append(frm)
        
            
                    # otherwise save the previous visit and create new lists of forms and cases for a new visit
                    else:
                        if prev_visited_forms:
                            previous_visit = self.create_visit(u, prev_visited_forms, prev_visited_cases)
                            
                        prev_visited_cases = form_cases
                        prev_visited_forms = [frm]
            
            # save the last visit for this user
            if prev_visited_forms:
                previous_visit = self.create_visit(u, prev_visited_forms, prev_visited_cases)      

        
