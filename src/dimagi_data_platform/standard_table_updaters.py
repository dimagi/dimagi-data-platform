'''
Created on Jun 17, 2014

@author: mel
'''
import logging

from peewee import prefetch

from dimagi_data_platform.data_warehouse_db import Domain, Sector, DomainSector, \
    Visit, Interaction, FormVisit, User, Form, CaseEvent, Cases, FormDefinition, \
    Subsector, FormDefinitionSubsector
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingDomainAnnotation, IncomingFormAnnotation


logger = logging.getLogger(__name__)

class StandardTableUpdater(object):
    '''
    updates a standard table from one or more incoming data tables produced by the importers
    '''
    
    def __init__(self, dbconn):
        self.conn = dbconn
    
    def update_table(self):
        pass
    

class DomainTableUpdater(StandardTableUpdater):
    '''
    updates the domain table, plus sectors and subsectors
    '''
    
    _first_col_names_to_skip = ['Total', 'Mean', 'STD']
    
    def __init__(self, dbconn):
        super(DomainTableUpdater, self).__init__(dbconn)
    
    def update_table(self):
        
        for row in IncomingDomain.select():
            attrs = row.attributes

            dname = attrs['Project']
            
            if dname not in self._first_col_names_to_skip:
                try:
                    domain = Domain.get(name=dname)
                except Domain.DoesNotExist:
                    domain = Domain.create(name=dname)
                
                domain.organization = attrs['Organization']
                domain.country = attrs['Deployment Country']
                domain.services = attrs['Services']
                domain.project_state = attrs['Project State']
                domain.attributes = attrs
                
                domain.save()
            
        for row in IncomingDomainAnnotation.select():
            
            attrs = row.attributes
            dname = attrs['Domain name']
            business_unit = attrs['Business unit']
            sector_names = [k.replace('Sector_', '') for k, v in attrs.iteritems() if (k.startswith('Sector_') & (v == 'Yes'))]
            
            try:
                domain = Domain.get(name=dname)
                domain.business_unit = business_unit
                if domain.attributes:
                    domain.attributes = domain.attributes.update(attrs)
                else:
                    domain.attributes = attrs
            
                for secname in sector_names:
                    try:
                        sec = Sector.get(name=secname)
                    except Sector.DoesNotExist:
                        sec = Sector(name=secname)
                        sec.save()
                    try:
                        ds = DomainSector.get(domain=domain, sector=sec)
                    except DomainSector.DoesNotExist:
                        ds = DomainSector(domain=domain, sector=sec)
                        ds.save()
            
            except Domain.DoesNotExist:
                logger.warn('Domain referenced i domain annotations table with name %s, does not exist' % (dname))
                
class FormDefTableUpdater(StandardTableUpdater):
    '''
    updates the form definition table, plus subsectors
    '''
    
    def __init__(self, dbconn):
        super(FormDefTableUpdater, self).__init__(dbconn)
    
    def update_table(self):
        
        for row in IncomingFormAnnotation.select():
            attrs = row.attributes
            xmlns = attrs['Form xmlns']
            app_id = attrs['Application ID']
            dname = attrs['Domain name']
            subsector_names = [k.replace('Subsector_', '') for k, v in attrs.iteritems() if (k.startswith('Subsector_') & (v == 'Yes'))]
            
            try:
                domain = Domain.get(name=dname)
                
                try:
                    fd = FormDefinition.get(xmlns=xmlns, app_id=app_id, domain=domain)
                except FormDefinition.DoesNotExist:
                    fd = FormDefinition(xmlns=xmlns, app_id=app_id, domain=domain)
                
                fd.attributes = attrs
                fd.save()
                
                for sname in subsector_names:
                    try:
                        sub = Subsector.get(name=sname)
                    except Subsector.DoesNotExist:
                        sub = Subsector(name=sname)
                        sub.save()
                    
                    try:
                        fs = FormDefinitionSubsector.get(formdef=fd, subsector=sub)     
                    except FormDefinitionSubsector.DoesNotExist:
                        fs = FormDefinitionSubsector(formdef=fd, subsector=sub)
                        fs.save()
                    
            except Domain.DoesNotExist:
                logger.warn('Domain with name %s does not exist, could not add Form Definition with xmlns %s and app ID %s' % (dname, xmlns, app_id))


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
                             "where domain.name like incoming_cases.domain and domain.id='%d');" % (self.domain.id, self.domain.id, self.domain.id))

class CasesTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        super(CasesTableUpdater, self).__init__(dbconn)
        
    def update_table(self):
        with self.conn.cursor() as curs:
            curs.execute("delete from cases where domain_id = '%d'" % self.domain.id)
            curs.execute("insert into cases (case_id, user_id, owner_id, parent_id, case_type, date_opened, date_modified, closed, date_closed, domain_id) "
                             "(select distinct on (case_id) case_id, a.id as user_id, b.id as owner_id, parent_id, case_type, date_opened, date_modified, closed, date_closed, domain.id "
                             "from incoming_cases, users as a, users as b, domain "
                             "where a.user_id = incoming_cases.user_id and b.user_id = incoming_cases.owner_id and a.domain_id = domain.id and b.domain_id = domain.id "
                             "and domain.name = incoming_cases.domain "
                             "and domain.id = '%d' "
                             "order by case_id, date_modified desc);" % (self.domain.id))
        
        

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
                "order by form_id, time_start);" % self.domain.id)

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
            curs.execute("delete from case_event where form_id in (select id from form where domain_id = '%d');" % self.domain.id)
            curs.execute("insert into case_event(form_id, case_id) (select form.id, cases.id from incoming_form, form, cases, domain "
                         "where incoming_form.form_id = form.form_id and incoming_form.case_id = cases.case_id "
                         " and domain.id = form.domain_id "
                         " and domain.id = cases.domain_id "
                         " and domain.id = '%d');" % self.domain.id)
        

class VisitTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''

    def __init__(self, dbconn, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)   
        self.home_visit_forms = [(fdef.xmlns, fdef.app_id) for fdef in self.domain.formdefs if fdef.attributes['Travel visit'] == 'Yes']
       
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
