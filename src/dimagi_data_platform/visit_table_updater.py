'''
Created on Jun 17, 2014

@author: mel
'''


from peewee import prefetch

from dimagi_data_platform.data_warehouse_db import User, Form, CaseEvent, Cases, \
    Visit, Interaction, FormVisit, Domain
from dimagi_data_platform.standard_table_updater import StandardTableUpdater


class VisitTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)   
        self.home_visit_forms = [(fdef.xmlns,fdef.app_id) for fdef in self.domain.formdefs if fdef.attributes['Travel visit']=='Yes']
       
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
            
            if (fp.xmlns, fp.app) in self.home_visit_forms:
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

        
