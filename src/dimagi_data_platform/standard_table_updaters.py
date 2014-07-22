'''
Created on Jun 17, 2014

@author: mel
'''
import datetime
import logging

from peewee import prefetch

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
        
        for row in IncomingDomain.get_unimported():
            attrs = row.attributes
            
            if not 'Project' in attrs:
                logger.warn('Must have Project to save domain, but we only have  %s' % attrs)
            else:
                dname = attrs['Project']
                
                if dname not in self._first_col_names_to_skip:
                    try:
                        domain = Domain.get(name=dname)
                    except Domain.DoesNotExist:
                        domain = Domain.create(name=dname)
                    
                    domain.organization = attrs['Organization'] if 'Organization' in attrs else None
                    domain.country = attrs['Deployment Country'] if 'Deployment Country' in attrs else None
                    domain.services = attrs['Services'] if 'Services' in attrs else None
                    domain.project_state = attrs['Project State'] if 'Project State' in attrs else None
                    domain.attributes = attrs
                    
                    domain.save()
            
        for row in IncomingDomainAnnotation.get_unimported():
            
            attrs = row.attributes
            
            if not 'Domain name' in attrs:
                logger.warn('Must have Domain name to save domain annotations, but we only have  %s' % attrs)
            else:
                dname = attrs['Domain name']
                business_unit = attrs['Business unit'] if 'Business unit' in attrs else None
                sector_names_annotations = [k.replace('Sector_', '') for k, v in attrs.iteritems() if (k.startswith('Sector_') & (v == 'Yes'))]
                
                sector_name_hq = [attrs["HQ_Sector"]] if "HQ_Sector" in attrs else []
                sector_names=list()+sector_name_hq+sector_names_annotations
                sector_names = [s for s in sector_names if (s is not None and not (s == ""))]
                
                try:
                    domain = Domain.get(name=dname)
                    domain.business_unit = business_unit
                    if domain.attributes:
                        domain.attributes = domain.attributes.update(attrs)
                    else:
                        domain.attributes = attrs
                    
                    
                    dq = DomainSector.delete().where(DomainSector.domain == domain)
                    dq.execute()
                    
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
                    domain.save()

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
        
        for row in IncomingFormAnnotation.get_unimported():
            attrs = row.attributes
            
            if not ('Form xmlns' in attrs and 'Application ID' in attrs and 'Domain name' in attrs):
                logger.warn('Must have Form xmlns, Application ID and Domain name to save form annotation, but we only have %s' % attrs)

            else:
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
                    
                    dq = FormDefinitionSubsector.delete().where(FormDefinitionSubsector.formdef == fd)
                    dq.execute()
                    
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
                    fd.save()
      
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
        
        case_users = IncomingCases.select(IncomingCases.user, IncomingCases.owner).where((IncomingCases.domain == self.domain.name) 
                                                                                         & ((IncomingCases.imported == False) | (IncomingCases.imported >> None)))
        
        form_users = IncomingForm.select(IncomingForm.user).where((IncomingForm.domain == self.domain.name) & ((IncomingForm.imported == False) | (IncomingForm.imported >> None)))
        incoming_user_ids = set([u.user for u in case_users] + [o.owner for o in case_users] + [f.user for f in form_users])
        
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
        inccases_q = IncomingCases.get_unimported(self.domain.name)
        logger.info('Incoming cases table has %d records not imported' % inccases_q.count())
        
        user_id_q = self.domain.users.select()
        user_id_dict = dict([(u.user, u) for u in user_id_q])
        
        cases_cur = Cases._meta.database.execute_sql('select case_id from cases '
                                                'where cases.domain_id = %d' % self.domain.id)
        existing_case_ids = [c[0] for c in cases_cur.fetchall()]
        
        insert_dicts = []
        update_dicts = []
        for inccase in inccases_q.iterator():
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
                    q = Cases.update(**row).where(Cases.case == row['case'])
                    q.execute()
                else:
                    insert_dicts.append(row)
            else:
                logger.warn("while inserting case with ID %s for domain %s couldn't find either the user or owner. user ID is %s, owner ID is %s" % (inccase.case, inccase.domain, inccase.user, inccase.owner))
        
        if insert_dicts:
            deduped = [dict(t) for t in set([tuple(d.items()) for d in insert_dicts])]
            logger.info("inserting %d cases for domain %s" % (len(deduped), self.domain.name))
            self.insert_chunked(deduped)

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
        incform_q = IncomingForm.get_unimported(self.domain.name)
        logger.info('Incoming form table has %d records not imported' % incform_q.count())
        
        user_id_q = self.domain.users.select()
        user_id_dict = dict([(u.user, u) for u in user_id_q])
        
        forms_cur = Form._meta.database.execute_sql('select form_id from form '
                                                'where form.domain_id = %d' % self.domain.id)
        existing_form_ids = [f[0] for f in forms_cur.fetchall()]
        
        insert_dicts = []
        
        for incform in incform_q.iterator():
            if incform.form not in existing_form_ids:
                if incform.user in user_id_dict:
                    start = datetime.datetime.strptime(incform.time_start, '%Y-%m-%dT%H:%M:%S') if incform.time_start else None
                    end = datetime.datetime.strptime(incform.time_end, '%Y-%m-%dT%H:%M:%S') if incform.time_end else None
                    rec = datetime.datetime.strptime(incform.received_on, '%Y-%m-%dT%H:%M:%S') if incform.received_on else None
                    
                    created = (incform.created == "True")
                    updated = (incform.updated == "True")
                    closed = (incform.closed == "True")
                    phone = (incform.is_phone_submission == "1.0")
                    
                    row = {'form':incform.form, 'xmlns':incform.xmlns, 'app':incform.app,
                           'time_start':start, 'time_end':end, 'received_on':rec,
                           'created':created, 'updated':updated, 'closed':closed,
                           'app_version':incform.app_version, 'is_phone_submission': phone,
                           'device':incform.device, 'user':user_id_dict[incform.user], 'domain':self.domain}
                    
                    insert_dicts.append(row)
                else:
                    logger.warn("while inserting form with ID %s for domain %s couldn't find user. user ID is %s" % (incform.form, incform.domain, incform.user))
        
        
        if insert_dicts:
            deduped = [dict(t) for t in set([tuple(d.items()) for d in insert_dicts])]
            logger.info("inserting %d forms for domain %s" % (len(deduped), self.domain.name))
            self.insert_chunked(deduped)


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
        ce_q = IncomingForm.select(IncomingForm.form, IncomingForm.case, IncomingForm.alt_case).where((IncomingForm.domain == self.domain.name) 
                                                                                                      & ((IncomingForm.imported == False) | (IncomingForm.imported >> None)))
        ce_pairs = set([(ce.form, ce.case if ce.case else ce.alt_case) for ce in ce_q.iterator()])
        
        cur = CaseEvent._meta.database.execute_sql('select form.form_id, cases.case_id '
                                                'from form, cases, case_event '
                                                'where form.id = case_event.form_id '
                                                'and cases.id = case_event.case_id '
                                                'and form.domain_id = %d' % self.domain.id)
        existing_pairs = set(cur.fetchall())
        pairs_to_insert = ce_pairs.difference(existing_pairs)
        insert_dicts = []
        
        forms_cur = CaseEvent._meta.database.execute_sql('select form.form_id, form.id from form '
                                                'where form.domain_id = %d' % self.domain.id)
        forms_dict = dict(set(forms_cur.fetchall()))
        
        cases_cur = CaseEvent._meta.database.execute_sql('select cases.case_id, cases.id from cases '
                                                'where cases.domain_id = %d' % self.domain.id)
        cases_dict = dict(set(cases_cur.fetchall()))
        
        for pair in pairs_to_insert:
            if pair[0] and pair[1]:
                if pair[0] in forms_dict and pair[1] in cases_dict: 
                    row = {'form':forms_dict[pair[0]], 'case':cases_dict[pair[1]]}
                    insert_dicts.append(row)
                else :
                    logger.error("while inserting case event, could not find either form %s or case %s in domain %s" 
                                 % (pair[0], pair[1], self.domain.name))
            
        
        if insert_dicts:
            logger.info("inserting %d case events for domain %s" % (len(insert_dicts), self.domain.name))
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
        super(VisitTableUpdater, self).__init__()
        
    def delete_most_recent(self, user):
        vq = Visit.select().where(Visit.user == user).order_by(Visit.time_start.desc()).limit(1)
        if vq.count() > 0:
            v = vq.get()
            logger.debug('deleting most recent visit for user %s with id %d, start time %s' % (user.id, v.id, v.time_start))
            dq = Visit.delete().where(Visit.id == v.id)
            dq.execute()
        else:
            logger.debug('no visits to delete for user %s' % user.id)
        
    def create_visit(self, user, visited_forms):
        
        time_start = min(visited_forms, key=lambda x : x.time_start).time_start
        time_end = max(visited_forms, key=lambda x : x.time_end).time_end
        v = Visit.create(user=user, time_start=time_start, time_end=time_end)
        
        ids = [f.id for f in visited_forms]
        uq = Form.update(visit=v).where(Form.id << ids)
        uq.execute()

        logger.debug('saved visit with id %d for user %s, %d forms' % (v.id, user.id, len(visited_forms)))
        
    def update_table(self):
        logger.info('TIMESTAMP starting visit table update for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        users = User.select().where(User.domain == self.domain).order_by(User.user)
        
        # dict with case event ids as keys, case_ids as values
        cur1 = CaseEvent._meta.database.execute_sql('select case_event.id, cases.case_id from cases, case_event '
                                                    'where case_event.case_id = cases.id '
                                                    'and cases.domain_id = %d' % self.domain.id)
        caseevent_caseid__dict = dict(set(cur1.fetchall()))
        
        # dict with case event ids as keys, case parents as values
        cur2 = CaseEvent._meta.database.execute_sql('select case_event.id, cases.parent_id from cases, case_event '
                                                    'where case_event.case_id = cases.id and cases.parent_id is not null '
                                                    'and cases.domain_id = %d' % self.domain.id)
        caseevent_parent_dict = dict(set(cur2.fetchall()))
        
        for usr in users:
            self.delete_most_recent(usr)
            
            logger.debug("getting visits for user %s" % usr.user)
            
            # forms already in visit
            prev_visited_forms = []
            # cases and parents of cases in forms already in visit
            prev_visited_case_ids = []
            
            forms = usr.forms.select().where(~(Form.time_end >> None) & ~(Form.time_start >> None) & (Form.visit >> None)).order_by(Form.time_start)
            ces = CaseEvent.select()
            forms_prefetch = prefetch(forms, ces)
            
            for frm in forms_prefetch:
                case_events = frm.caseevents_prefetch
                if len(case_events) > 0:
                    # cases updated in this form
                    form_case_ids = [caseevent_caseid__dict[cec.id] for cec in case_events if cec.id in caseevent_caseid__dict]
                    # parents of cases updated in this form
                    form_case_parents = [caseevent_parent_dict[cec.id] for cec in case_events if cec.id in caseevent_parent_dict]
                    
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
                            previous_visit = self.create_visit(usr, prev_visited_forms)
                            
                        prev_visited_case_ids = form_case_ids + form_case_parents
                        prev_visited_forms = [frm]
            
            # save the last visit for this user
            if prev_visited_forms:
                previous_visit = self.create_visit(usr, prev_visited_forms)      
