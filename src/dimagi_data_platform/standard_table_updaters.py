'''
Created on Jun 17, 2014

@author: mel
'''
import datetime
import logging

from peewee import prefetch, UpdateQuery

from dimagi_data_platform.data_warehouse_db import Domain, Sector, DomainSector, \
     User, Form, CaseEvent, Cases, FormDefinition, \
    Subsector, FormDefinitionSubsector, Visit
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingDomainAnnotation, IncomingFormAnnotation, IncomingCases, \
    IncomingForm
from dimagi_data_platform.utils import break_into_chunks


logger = logging.getLogger(__name__)


class StandardTableUpdater(object):
    '''
    updates a standard table from one or more incoming data tables produced by the importers
    '''
    _db_warehouse_table_class = None
    
    def __init__(self):
        super(StandardTableUpdater, self).__init__()
    
    def update_table(self):
        pass
    
    def insert_chunked(self, l):
        
        if not self._db_warehouse_table_class:
            logger.error('No data warehouse table class is defined, cannot insert chunked list')
        else:
            chunks = break_into_chunks(l, 5000)
            count = 0
            for chunk in chunks:
                count = count + 1
                logger.info('inserting chunk %d of %d' % (count, len(chunks)))
                self._db_warehouse_table_class.insert_many(chunk).execute()
    

class DomainTableUpdater(StandardTableUpdater):
    '''
    updates the domain table, plus sectors and subsectors
    '''
    _db_warehouse_table_class = Domain
    _first_col_names_to_skip = ['Total', 'Mean', 'STD']
    
    def __init__(self):
        super(DomainTableUpdater, self).__init__()
    
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
    _db_warehouse_table_class = FormDefinition
    
    def __init__(self):
        super(FormDefTableUpdater, self).__init__()
    
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
    updates the user table from incoming forms and cases
    '''
    
    _db_warehouse_table_class = User
    

    def __init__(self, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        super(UserTableUpdater, self).__init__()
        
    def update_table(self):
        logger.info('TIMESTAMP starting user table update for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        
        case_users = IncomingCases.select(IncomingCases.user).where(IncomingCases.domain == self.domain.name)
        case_owners = IncomingCases.select(IncomingCases.owner).where(IncomingCases.domain == self.domain.name)
        form_users = IncomingForm.select(IncomingForm.user).where(IncomingForm.domain == self.domain.name)
        incoming_user_ids = set([u.user for u in case_users] + [o.owner for o in case_owners] + [f.user for f in form_users])
        
        existing_user_ids = set([u.user for u in self.domain.users])
        user_ids_to_create = incoming_user_ids.difference(existing_user_ids)
        
        for user_id in user_ids_to_create:
            new_user = User.create(user=user_id, domain=self.domain)

class CasesTableUpdater(StandardTableUpdater):
    '''
    updates the case table from incoming cases
    '''
    
    _db_warehouse_table_class = Cases

    def __init__(self, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        super(CasesTableUpdater, self).__init__()
        
    def update_table(self):
        logger.info('TIMESTAMP starting cases table update for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        inccases_q = IncomingCases.select().where(IncomingCases.domain == self.domain.name)
        
        user_id_q = self.domain.users.select()
        user_id_dict = dict([(u.user, u) for u in user_id_q])
        
        existing_cases = self.domain.cases
        existing_case_ids = [c.case for c in existing_cases]
        
        insert_dicts = []
        update_dicts = []
        for inccase in inccases_q:
            if inccase.user in user_id_dict and inccase.owner in user_id_dict:
                
                # note different date formats for these
                opened = datetime.datetime.strptime(inccase.date_opened, '%Y-%m-%dT%H:%M:%S') if inccase.date_opened else None
                modified = datetime.datetime.strptime(inccase.date_modified, '%Y-%m-%d %H:%M:%S') if inccase.date_modified else None
                closed = datetime.datetime.strptime(inccase.date_closed, '%Y-%m-%d %H:%M:%S') if inccase.date_closed else None
                    
                is_closed = inccase.closed == 'True'
                
                row = {'case':inccase.case, 'user':user_id_dict[inccase.user], 'owner': user_id_dict[inccase.owner],
                       'parent':inccase.parent, 'case_type':inccase.case_type, 'date_opened':opened, 'date_modified': modified,
                       'date_closed':closed, 'closed':is_closed, 'domain':self.domain}
                
                if inccase.case in existing_case_ids:
                    update_dicts.append(row)
                else:
                    insert_dicts.append(row)
            else:
                logger.warn("while inserting case with ID %s for domain %s couldn't find either the user or owner. user ID is %s, owner ID is %s" % (inccase.case, inccase.domain, inccase.user, inccase.owner))
        
        if insert_dicts:
            deduped = [dict(t) for t in set([tuple(d.items()) for d in insert_dicts])]
            self.insert_chunked(deduped)
            
        for row in update_dicts:
            q = Cases.update(**row).where(Cases.case == row['case'])
            q.execute()
        
        

class FormTableUpdater(StandardTableUpdater):
    '''
    updates the form table from incoming forms
    '''
    
    _db_warehouse_table_class = Form

    def __init__(self, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        super(FormTableUpdater, self).__init__()
        
    def update_table(self):
        logger.info('TIMESTAMP starting form table update for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        incform_q = IncomingForm.select().where(IncomingForm.domain == self.domain.name)
        
        user_id_q = self.domain.users.select()
        user_id_dict = dict([(u.user, u) for u in user_id_q])
        
        existing_forms = self.domain.forms
        existing_form_ids = [f.form for f in existing_forms]
        
        insert_dicts = []
        update_dicts = []
        for incform in incform_q:
            if incform.user in user_id_dict:
                start = datetime.datetime.strptime(incform.time_start, '%Y-%m-%dT%H:%M:%S') if incform.time_start else None
                end = datetime.datetime.strptime(incform.time_end, '%Y-%m-%dT%H:%M:%S') if incform.time_end else None
                row = {'form':incform.form, 'xmlns':incform.xmlns, 'app':incform.app,
                       'time_start':start, 'time_end':end, 'user':user_id_dict[incform.user], 'domain':self.domain}
                
                if incform.form in existing_form_ids:
                    update_dicts.append(row)
                else:
                    insert_dicts.append(row)
            else:
                logger.warn("while inserting form with ID %s for domain %s couldn't find user. user ID is %s" % (incform.form, incform.domain, incform.user))
        
        
        if insert_dicts:
            deduped = [dict(t) for t in set([tuple(d.items()) for d in insert_dicts])]
            self.insert_chunked(deduped)
        
        for row in update_dicts:
            logger.debug('updating form with dict %s' % row)
            q = Form.update(**row).where(Form.form == row['form'])
            q.execute()


class CaseEventTableUpdater(StandardTableUpdater):
    '''
    updates the case event table from incoming forms
    '''
    
    _db_warehouse_table_class = CaseEvent

    def __init__(self, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        super(CaseEventTableUpdater, self).__init__()
        
    def update_table(self):
        logger.info('TIMESTAMP starting case event table update for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        ce_q = IncomingForm.select(IncomingForm.form, IncomingForm.case).where(IncomingForm.domain == self.domain.name)
        
        form_id_q = self.domain.forms.select(Form.id, Form.form)
        form_id_dict = dict([(f.form, f) for f in form_id_q])
        
        case_id_q = self.domain.cases.select(Cases.id, Cases.case)
        case_id_dict = dict([(c.case, c) for c in case_id_q])
        
        existing_formcase_pairs = [(ce.form.form,ce.case.case) for ce in CaseEvent.select().join(Form).where(Form.domain == self.domain).join(Cases, on=(CaseEvent.case==Cases.id))]
        
        insert_dicts = []
        for ce_attrs in ce_q:
            if ce_attrs.form in form_id_dict and ce_attrs.case in case_id_dict:
                row = {'form':form_id_dict[ce_attrs.form], 'case':case_id_dict[ce_attrs.case]}
                
                if (ce_attrs.form,ce_attrs.case) not in existing_formcase_pairs:
                    insert_dicts.append(row)
        
        if insert_dicts:
            self.insert_chunked(insert_dicts)

class VisitTableUpdater(StandardTableUpdater):
    '''
    updates the user table from form data
    '''
    
    _db_warehouse_table_class = Visit

    def __init__(self, domain):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)   
        self.home_visit_forms = [(fdef.xmlns, fdef.app_id) for fdef in self.domain.formdefs if fdef.attributes['Travel visit'] == 'Yes']
       
        super(VisitTableUpdater, self).__init__()
        
    def delete_most_recent(self, user):
        vq = Visit.select().where(Visit.user == user).order_by(Visit.time_start.desc()).limit(1)
        if vq.count() > 0:
            v = vq.get()
            logger.info('deleting most recent visit for user %s with id %d, start time %s' % (user.id, v.id, v.time_start))
            dq = Visit.delete().where(Visit.id == v.id)
            dq.execute()
        else:
            logger.info('no visits to delete for user %s' % user.id)
        
    def create_visit(self, user, visited_forms):
        
        v = Visit.create(user=user)
        v.home_visit = False
        
        for fp in visited_forms:
            fp.visit = v
            if (fp.xmlns, fp.app) in self.home_visit_forms:
                v.home_visit = True
            fp.save()
        v.time_start = min(visited_forms, key=lambda x : x.time_start).time_start
        v.time_end = max(visited_forms, key=lambda x : x.time_end).time_end
     
        v.save()
        logger.debug('saved visit with id %d for user %s, %d forms' % (v.id,user.id,len(visited_forms)))
        
    def update_table(self):
        logger.info('TIMESTAMP starting visit table update for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        users = User.select().where(User.domain == self.domain).order_by(User.user)
        
        for usr in users:
            self.delete_most_recent(usr)
        
        forms = Form.select().where(~(Form.time_end >> None) & ~(Form.time_start >> None) & (Form.visit >> None)).order_by(Form.time_start)
        ces = CaseEvent.select().join(Cases)
        
        users_prefetch = prefetch(users, forms, ces)
         
        for u in users_prefetch:
            logger.info("getting visits for user %s" % u.user)
            
            # forms already in visit
            prev_visited_forms = []
            # cases and parents of cases in forms already in visit
            prev_visited_case_ids = []
            
            for frm in u.forms_prefetch:
                
                case_events = frm.caseevents_prefetch
                if len(case_events) > 0:
                    # cases updated in this form
                    form_case_ids = [cec.case.case for cec in case_events]
                    # parents of cases updated in this form
                    form_case_parents = [cec.case.parent for cec in case_events if cec.case.parent]
                    
                    # if cases in this form have parents or are parents of cases already in this visit, add this form to the visit
                    if len(set(form_case_ids) & set(prev_visited_case_ids)) > 0:
                        prev_visited_case_ids = prev_visited_case_ids + form_case_ids + form_case_parents
                        prev_visited_forms.append(frm)
                    
                    # if parents of cases in this form have parents or are parents of cases already in this visit, add this form to the visit
                    elif len(set(prev_visited_case_ids) & set(form_case_parents)) > 0:
                        prev_visited_case_ids = prev_visited_case_ids + form_case_ids + form_case_parents
                        prev_visited_forms.append(frm)
            
                    # otherwise save the previous visit and create new lists of forms and cases for a new visit
                    else:
                        if prev_visited_forms:
                            previous_visit = self.create_visit(u, prev_visited_forms)
                            
                        prev_visited_case_ids = form_case_ids + form_case_parents
                        prev_visited_forms = [frm]
            
            # save the last visit for this user
            if prev_visited_forms:
                previous_visit = self.create_visit(u, prev_visited_forms)      
