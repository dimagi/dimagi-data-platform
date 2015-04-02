'''
Created on Jun 17, 2014

@author: mel
'''
from datetime import timedelta
import datetime
import json
import logging

from pandas.core.frame import DataFrame
from peewee import prefetch
import requests
from requests.auth import HTTPDigestAuth
from requests.exceptions import RequestException
import slumber
import sqlalchemy

from dimagi_data_platform import conf, data_warehouse_db
from dimagi_data_platform.data_warehouse_db import Domain, Sector, DomainSector, \
     User, Form, CaseEvent, Cases, FormDefinition, \
    Subsector, FormDefinitionSubsector, Visit, DomainSubsector, WebUser, \
    DeviceLog, Application, MobileUser, MobileUserDomain, WebUserDomain
from dimagi_data_platform.incoming_data_tables import IncomingDomain, \
    IncomingDomainAnnotation, IncomingFormAnnotation, IncomingCases, \
    IncomingForm, IncomingFormDef, IncomingUser, IncomingWebUser, \
    IncomingDeviceLog, IncomingSalesforceRecord, IncomingApplication
from dimagi_data_platform.utils import break_into_chunks, dict_flatten, \
    dict_str_vals


logger = logging.getLogger(__name__)


class Loader(object):
    '''
    loads data into a standard table from one or more incoming data tables produced by the extracters
    '''
    _db_warehouse_table_class = None
    
    def __init__(self):
        super(Loader, self).__init__()
        pass
    
    def do_load(self):
        pass
    
    def insert_chunked(self, l, db_table_class=None):
        
        if (not db_table_class and not self._db_warehouse_table_class):
            logger.error('No data warehouse table class is defined and none supplies, cannot insert chunked list')
        else:
            if db_table_class is None:
                db_table_class = self._db_warehouse_table_class
            chunks = break_into_chunks(l, 5000)
            count = 0
            for chunk in chunks:
                count = count + 1
                logger.info('inserting chunk %d of %d' % (count, len(chunks)))
                db_table_class.insert_many(chunk).execute()
    

class DomainLoader(Loader):
    '''
    loads data to the domain table, plus sectors and subsectors
    '''
    _db_warehouse_table_class = Domain
    _first_col_names_to_skip = ['Total', 'Mean', 'STD']
    
    def __init__(self):
        super(DomainLoader, self).__init__()
        
    def update_sectors(self, domain, sector_list, subsector_list):
        
        for secname in sector_list:
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
        
        for subname in subsector_list:
            try:
                sub = Subsector.get(name=subname)
            except Subsector.DoesNotExist:
                sub = Subsector(name=subname)
                sub.save()
            try:
                ds = DomainSubsector.get(domain=domain, subsector=sub)
            except DomainSubsector.DoesNotExist:
                ds = DomainSubsector(domain=domain, subsector=sub)
                ds.save()
    
    def load_annotations(self):
        annotations = IncomingDomainAnnotation.get_unimported()
        for row in annotations:
            if not row.attributes:
                continue
            
            attrs = row.attributes
            if not 'Domain name' in attrs:
                logger.error('Must have Domain name to save domain, but we only have  %s' % attrs)
                continue
            else:
                dname = attrs['Domain name']
                
            try:
                domain = Domain.get(name=dname)
            except Domain.DoesNotExist:
                logger.error('Cannot save domain annotations, no domain named  %s' % dname)
                continue
            
            domain.organization = attrs['Organization'] if 'Organization' in attrs else None
            domain.country = attrs['Deployment Country'] if 'Deployment Country' in attrs else None
            domain.services = attrs['Services'] if 'Services' in attrs else None
            domain.project_state = attrs['Project State'] if 'Project State' in attrs else None
            domain.business_unit = attrs['Business unit'] if 'Business unit' in attrs else None
            if 'Test Project?' in attrs:
                if (attrs['Test Project?'].lower() == "true"):
                    domain.test = True
                elif (attrs['Test Project?'].lower() == "false"):
                    domain.test = False
            if 'Active?' in attrs:
                domain.active = (attrs['Active?'].lower() == "true")
                
            domain.attributes.update(attrs)
            
            sector_names_annotations = [k.replace('Sector_', '') for k, v in attrs.iteritems() if (k.startswith('Sector_') & (v == 'Yes'))]
            sector_names = [s for s in sector_names_annotations if (s is not None and not (s == "") and not (s == "No info"))]
            
            subsector_names_annotations = [k.replace('Sub-Sector_', '') for k, v in attrs.iteritems() if (k.startswith('Sub-Sector_') & (v == 'Yes'))]
            subsector_names = [sb for sb in subsector_names_annotations if (sb is not None and not (sb == "") and not (sb == "No info"))]
            
            self.update_sectors(domain, sector_names, subsector_names)
            
            domain.save()
            
    def load_api_data(self):
        hq_domain_list = IncomingDomain.get_unimported()
        for row in hq_domain_list:
            if not row.api_json:
                logger.warn('IncomingDomain entry with no data for api_json  %s' % row)
                continue
            api_data = row.api_json
            dname = api_data['domain_properties']['name']
            try:
                domain = Domain.get(name=dname)
            except Domain.DoesNotExist:
                logger.info('Adding new domain named  %s' % dname)
                domain = Domain.create(name=dname)
            domain.organization = api_data['domain_properties']['organization']
            domain.countries = api_data['domain_properties']['deployment']['countries']
            domain.services = api_data['domain_properties']['internal']['services']
            domain.project_state = api_data['domain_properties']['internal']['project_state']
            domain.active = api_data['calculated_properties']['cp_is_active'] if ('calculated_properties' in api_data and 'cp_is_active' in api_data['calculated_properties']) else None
            domain.test = (api_data['domain_properties']['is_test'] == "true") if (api_data['domain_properties']['is_test'] != "none") else None
            
            # delete all sector information for this domain
            dq_sec = DomainSector.delete().where(DomainSector.domain == domain)
            dq_sec.execute()
            dq_subsec = DomainSubsector.delete().where(DomainSubsector.domain == domain)
            dq_subsec.execute()
            
            sector = api_data['domain_properties']['internal']['area']
            sub_sector = api_data['domain_properties']['internal']['sub_area']
            if sector or sub_sector:
                self.update_sectors(domain, [sector], [sub_sector])
            
            # add billing prefix only, domain properties prefix is nothing, calculate properties all have cpp_
            billing_properties = dict_flatten(api_data['billing_properties'])
            domain.attributes = dict(('billing_%s' % key, value) for (key, value) in billing_properties.iteritems())
            domain.attributes.update(dict_flatten(api_data['calculated_properties']))
            domain.attributes.update(dict_flatten(api_data['domain_properties']))
            domain.attributes = dict_str_vals(domain.attributes)
            domain.save()
    
    def do_load(self):
        self.load_api_data()
        self.load_annotations()
                
                
class ApplicationLoader(Loader):
    '''
    loads data to the application table
    '''
    _db_warehouse_table_class = Application
    
    def __init__(self, domain):
        self.domain = Domain.get(name=domain)
        super(ApplicationLoader, self).__init__()
        
    def do_load(self):
        for inc in IncomingApplication.get_unimported(self.domain.name):
            if not inc.app_id:
                continue
            
            try:
                app = Application.get(Application.app_id == inc.app_id, Application.domain == self.domain.id)
            except Application.DoesNotExist:
                app = Application(app_id=inc.app_id, domain=self.domain.id)
            
            app.app_name = inc.app_name
            app.attributes = dict_flatten(inc.attributes_json)
            app.attributes = dict_str_vals(app.attributes)
            app.save()
                
class FormDefLoader(Loader):
    '''
    loads data to the form definition table, plus subsectors
    '''
    _db_warehouse_table_class = FormDefinition
    
    def __init__(self, domain):
        self.domain = Domain.get(name=domain)
        super(FormDefLoader, self).__init__()
        
    def load_from_annotations(self):
        for row in IncomingFormAnnotation.get_unimported().select().where(IncomingFormAnnotation.attributes.contains({'Domain name': self.domain.name})):
            attrs = row.attributes
            
            if not ('Form xmlns' in attrs and 'Domain name' in attrs):
                logger.warn('Must have Form xmlns and Domain name to save form annotation, but we only have %s' % attrs)

            else:
                xmlns = attrs['Form xmlns']
                app_id = attrs['Application ID'] if 'Application ID' in attrs else None
                dname = attrs['Domain name']
                subsector_names = [k.replace('Subsector_', '') for k, v in attrs.iteritems() if (k.startswith('Subsector_') & (v == 'Yes'))]
                
                try:
                    domain = Domain.get(name=dname)
                    application = Application.get_by_app_id_str(app_id, domain)
                    try:
                        if not application:
                            fd = FormDefinition.get(FormDefinition.xmlns == xmlns, FormDefinition.application >> None, FormDefinition.domain == domain)
                        else:
                            fd = FormDefinition.get(FormDefinition.xmlns == xmlns, FormDefinition.application == application, FormDefinition.domain == domain)
                    except FormDefinition.DoesNotExist:
                        fd = FormDefinition(xmlns=xmlns, application=application, domain=domain)
                    
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

        
    def load_from_API(self):
        for inc in IncomingFormDef.get_unimported(self.domain.name):
            if not inc.form_xmlns:
                logger.warn('Formdef with no xmlns not added for domain %s ' % (self.domain.name))
                continue
            
            domain = Domain.get(name=inc.domain)
            application = Application.get_by_app_id_str(inc.app_id, domain)

            try:
                if not (inc.app_id):
                    fd = FormDefinition.get(FormDefinition.xmlns == inc.form_xmlns, FormDefinition.application >> None, FormDefinition.domain == domain)
                else:
                    fd = FormDefinition.get(FormDefinition.xmlns == inc.form_xmlns, FormDefinition.application == application, FormDefinition.domain == domain)
            except FormDefinition.DoesNotExist:
                fd = FormDefinition(xmlns=inc.form_xmlns, application=application, domain=domain)
                
            fd.form_names = json.loads(inc.form_names) if inc.form_names else None
            fd.formdef_json = json.loads(inc.formdef_json)
            
            fd.save()
    
    def do_load(self):
        self.load_from_API()
        self.load_from_annotations()
        
class WebUserLoader(Loader):
    '''
    loads data to the user table from the web-user API (incoming web users)
    '''
    
    _db_warehouse_table_class = WebUser
    domain = None
    
    def __init__(self, domain=None):
        '''
        Constructor
        '''
        if domain:
            self.domain = Domain.get(name=domain)
        super(WebUserLoader, self).__init__()
    
    def load_from_API(self):
        for inc in IncomingWebUser.get_unimported(self.domain.name if self.domain else None):
            try:
                u = User.get(user_id=inc.api_id)
            except User.DoesNotExist:
                logger.info('creating new web user for user_id %s' % inc.api_id)
                u = User.create(user_id=inc.api_id)
                
            u.username = inc.username
            u.first_name = inc.first_name
            u.last_name = inc.last_name
            u.default_phone_number = inc.default_phone_number
            u.email = inc.email
            u.phone_numbers = inc.phone_numbers.split(',') if inc.phone_numbers else None
            u.save()

            try:
                wu = WebUser.get(user=u)
            except WebUser.DoesNotExist:
                logger.info('creating new web user for user_id %s' % inc.api_id)
                wu = WebUser.create(user=u)
            wu.is_superuser = '@dimagi.com' in u.username
            wu.save()
            
            if inc.domain:
                try:
                    try:
                        domain = Domain.get(name=inc.domain)
                        du = WebUserDomain.get(web_user=u.id, domain=domain)
                    except WebUserDomain.DoesNotExist:
                        logger.info('creating new web user domain link for user_id %s' % inc.api_id)
                        du = WebUserDomain.create(web_user=u.id, domain=domain)
                    # if no info is available, keep the values we have
                    du.webuser_role = inc.webuser_role if inc.webuser_role else du.webuser_role
                    du.resource_uri = inc.resource_uri if inc.resource_uri else du.resource_uri
                    du.is_admin = inc.is_admin if inc.is_admin else du.is_admin
                    du.save()
                    
                except Domain.DoesNotExist:
                    logger.error('No domain found with name %s' % inc.domain)
            
    def do_load(self):
        logger.info('TIMESTAMP starting web user table load %s' % datetime.datetime.now())
        self.load_from_API()
            

class UserLoader(Loader):
    '''
    loads data to the user table from the user API (incoming users) and then add any extra userids from incoming forms and cases
    '''
    
    _db_warehouse_table_class = User
    
    def __init__(self, domain, api_version, username, password):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        self.api_version = api_version
        self.username = username
        self.password = password
        super(UserLoader, self).__init__()
        
    def fetch_incoming_from_api(self, user_id):
        url = 'https://www.commcarehq.org/a/%s/api/%s/user/%s' % (self.domain.name, self.api_version, user_id)
        try:
            req = requests.get(url, auth=HTTPDigestAuth(self.username, self.password))
            if req.status_code == 200:
                user_dict = req.json()
                user_dict['user_id'] = user_dict['id'] 
                user_dict.pop('id', None)  # don't keep the ID in or it will try to set the autogenerated db id
                user_dict['phone_numbers'] = ','.join(user_dict['phone_numbers'])
                user_dict['groups'] = ','.join(user_dict['groups'])
                user_dict['domain'] = self.domain.name
                user_dict['imported'] = False
                
                incoming_user = IncomingUser.create(**user_dict)
                return(incoming_user)
            
            elif req.status_code == 400:
                logger.warn('API returned not found for user_id %s' % user_id)
            else:
                logger.warn('Got status code %s trying to retrieve user data by user_id, url was %s' % (req.status_code, url))
        except RequestException, e:
            logger.warn('Error trying to retrieve user data by user_id')
            logger.exception(e)
        
    def create_missing(self, user_id):
        new_incoming_user = self.fetch_incoming_from_api(user_id)
        if new_incoming_user:
            new_incoming_user.deleted = True  # must be deleted, because this user was missing from the user API call results
            new_incoming_user.save()
            
            new_user_id = self.create_or_update_user(new_incoming_user)
            
            new_incoming_user.imported = True
            new_incoming_user.save()
            return new_user_id
        else:
            try:
                u = User.get(user_id=user_id)
            except User.DoesNotExist:
                logger.info('creating new user for user_id %s' % user_id)
                u = User.create(user_id=user_id)
            return u.id
        
    def create_or_update_user(self, incoming_user): 
        try:
            u = User.get(user_id=incoming_user.user_id)
        except User.DoesNotExist:
            logger.info('creating new user for user_id %s' % incoming_user.user_id)
            u = User.create(user_id=incoming_user.user_id)
        u.username = incoming_user.username
        u.first_name = incoming_user.first_name
        u.last_name = incoming_user.last_name
        u.default_phone_number = incoming_user.default_phone_number
        u.email = incoming_user.email
        u.phone_numbers = incoming_user.phone_numbers.split(',') if incoming_user.phone_numbers else None
        u.save()
        
        try:
            mu = MobileUser.get(user=u)
        except MobileUser.DoesNotExist:
            logger.info('creating new mobile user for user_id %s' % incoming_user.user_id)
            mu = MobileUser.create(user=u)
        mu.groups = incoming_user.groups.split(',') if incoming_user.groups else None
        mu.completed_last_30 = incoming_user.completed_last_30
        mu.submitted_last_30 = incoming_user.submitted_last_30
        mu.deactivated = incoming_user.deactivated
        mu.deleted = incoming_user.deleted
        mu.save()
        
        try:
            du = MobileUserDomain.get(mobile_user=u.id, domain=self.domain)
        except MobileUserDomain.DoesNotExist:
            logger.info('creating new mobile user domain link for user_id %s' % incoming_user.user_id)
            du = MobileUserDomain.create(mobile_user=u.id, domain=self.domain)
            du.save()
        return u.id
    
    def load_from_API(self):
        incoming_users = IncomingUser.get_unimported(self.domain.name)
        
        for inc in incoming_users:
            self.create_or_update_user(inc)
        
        # check if there are any users we have previously got data on for this domain but who no longer show in results.
        domain_mobile_users = User.select(User.user_id).join(MobileUser).join(MobileUserDomain).where((MobileUserDomain.domain == self.domain) & ~(MobileUser.deleted))
        existing_user_ids = [exu.user_id for exu in domain_mobile_users if exu.user_id is not None]
        incoming_user_ids = [inc.user_id for inc in incoming_users if inc.user_id is not None]
        missing_user_ids = list(set(existing_user_ids) - set(incoming_user_ids))
        
        # for each of these, check if they are deleted
        if missing_user_ids:
            logger.info('There are %d users in the database whose data was not returned by the API; checking if deleted' % len(missing_user_ids))
        
            for user_id in missing_user_ids:
                new_incoming_user = self.fetch_incoming_from_api(user_id)
                if new_incoming_user:
                    new_incoming_user.deleted = True  # must be deleted, because not in this list of API call results
                    new_incoming_user.save()
                    logger.info('Updating user %s deleted to True' % user_id)
                    self.create_or_update_user(new_incoming_user)
                    new_incoming_user.imported = True
                    new_incoming_user.save()
            
    def do_load(self):
        logger.info('TIMESTAMP starting user table load for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        self.load_from_API()

class CasesLoader(Loader):
    '''
    loads data to the case table from incoming cases
    '''
    
    _db_warehouse_table_class = Cases

    def __init__(self, domain, user_loader):
        '''
        Constructor
        '''
        self.user_loader = user_loader
        self.domain = Domain.get(name=domain)
        super(CasesLoader, self).__init__()
        
    def do_load(self):
        logger.info('TIMESTAMP starting cases table load for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        inccases_q = IncomingCases.get_unimported(self.domain.name).select().distinct([IncomingCases.case]).order_by(IncomingCases.id.desc())
        logger.info('Incoming cases table has %d records not imported' % inccases_q.count())
        
        user_id_q = User.select()
        user_id_dict = dict([(u.user_id, u) for u in user_id_q])
        
        cases_cur = Cases._meta.database.execute_sql('select case_id, date_modified from cases '
                                                'where cases.domain_id = %d' % self.domain.id)
        case_updates_dict = dict([(c[0], c[1]) for c in cases_cur.fetchall()])
        existing_case_ids = case_updates_dict.keys()
        
        insert_dicts = []
        for inccase in inccases_q.iterator():
            if not inccase.user in user_id_dict:
                logger.warn("while inserting case with ID %s for domain %s couldn't find user with user ID %s" % (inccase.case, inccase.domain, inccase.user))
                new_id = self.user_loader.create_missing(inccase.user)
                user_id_dict[inccase.user] = new_id
                
            if not inccase.owner in user_id_dict:
                logger.warn("while inserting case with ID %s for domain %s couldn't find owner user with user ID %s" % (inccase.case, inccase.domain, inccase.owner))
                new_id = self.user_loader.create_missing(inccase.owner)
                user_id_dict[inccase.owner] = new_id
                
            # note different date formats for these
            opened = datetime.datetime.strptime(inccase.date_opened, '%Y-%m-%dT%H:%M:%S') if inccase.date_opened else None
            modified = datetime.datetime.strptime(inccase.date_modified, '%Y-%m-%d %H:%M:%S') if inccase.date_modified else None
            closed = datetime.datetime.strptime(inccase.date_closed, '%Y-%m-%d %H:%M:%S') if inccase.date_closed else None
                
            is_closed = inccase.closed == 'True'
            
            row = {'case':inccase.case, 'user':user_id_dict[inccase.user], 'owner': user_id_dict[inccase.owner],
                   'parent':inccase.parent, 'case_type':inccase.case_type, 'date_opened':opened, 'date_modified': modified,
                   'date_closed':closed, 'closed':is_closed, 'domain':self.domain}
            
            # update if we have the case already and the modification date is greater than the last modification
            if inccase.case in existing_case_ids:
                if (case_updates_dict[inccase.case] is None) or (modified > case_updates_dict[inccase.case]):
                    q = Cases.update(**row).where(Cases.case == row['case'])
                    q.execute()
            # else just include in bulk insert
            else:
                insert_dicts.append(row)
                
        if insert_dicts:
            deduped = [dict(t) for t in set([tuple(d.items()) for d in insert_dicts])]
            logger.info("inserting %d cases for domain %s" % (len(deduped), self.domain.name))
            self.insert_chunked(deduped)

class FormLoader(Loader):
    '''
    loads data to the form table from incoming forms
    '''
    
    _db_warehouse_table_class = Form

    def __init__(self, domain, user_loader):
        '''
        Constructor
        '''
        self.user_loader = user_loader
        self.domain = Domain.get(name=domain)
        super(FormLoader, self).__init__()
        
    def load_forms(self):
        logger.info('TIMESTAMP starting form table load for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        incform_q = IncomingForm.get_unimported(self.domain.name).select().distinct([IncomingForm.form]).where(IncomingForm.record_type == 'form').order_by(IncomingForm.id.desc())
        logger.info('Incoming form table has %d records not imported' % incform_q.count())
        
        user_id_q = User.select()
        user_id_dict = dict([(u.user_id, u) for u in user_id_q])
        
        forms_cur = Form._meta.database.execute_sql('select form_id from form '
                                                'where form.domain_id = %d' % self.domain.id)
        existing_form_ids = [f[0] for f in forms_cur.fetchall()]
        
        insert_dicts = []
        
        for incform in incform_q.iterator():
            if incform.form and incform.form not in existing_form_ids:
                if not incform.user in user_id_dict:
                    logger.warn("while inserting form with ID %s for domain %s couldn't find user with user ID %s" % (incform.form, incform.domain, incform.user))
                    new_id = self.user_loader.create_missing(incform.user)
                    user_id_dict[incform.user] = new_id
                
                start = datetime.datetime.strptime(incform.time_start, '%Y-%m-%dT%H:%M:%S') if incform.time_start else None
                end = datetime.datetime.strptime(incform.time_end, '%Y-%m-%dT%H:%M:%S') if incform.time_end else None
                rec = datetime.datetime.strptime(incform.received_on, '%Y-%m-%dT%H:%M:%S') if incform.received_on else None
                
                # adjust form end to time received at server if > 30 days between the two
                if (not (None in (start,end,rec))) and (abs((rec-end).days) > 30):
                    end = rec
                    start = rec-(end-start)
                
                phone = (incform.is_phone_submission == "1.0")
                
                application = Application.get_by_app_id_str(incform.app, self.domain)
                application_id = application.id if application else None
                
                formdef = FormDefinition.get_by_xmlns_and_application(incform.xmlns, application, self.domain)
                formdef_id = formdef.id if formdef else None
                
                row = {'form':incform.form, 'formdef':formdef_id, 'application':application_id,
                       'time_start':start, 'time_end':end, 'received_on':rec,
                       'app_version':incform.app_version, 'is_phone_submission': phone,
                       'device':incform.device, 'user':user_id_dict[incform.user], 'domain':self.domain}
                
                insert_dicts.append(row)
        
        if insert_dicts:
            deduped = [dict(t) for t in set([tuple(d.items()) for d in insert_dicts])]
            logger.info("inserting %d forms for domain %s" % (len(deduped), self.domain.name))
            self.insert_chunked(deduped)
        
    def do_load(self):
        self.load_forms()
        
class CaseEventLoader(Loader):
    '''
    loads data to the caseevent table form incoming forms
    '''
    _db_warehouse_table_class = CaseEvent
    
    def __init__(self, domain):
        self.domain = Domain.get(name=domain)
        super(CaseEventLoader, self).__init__()
        
    def do_load(self):
        logger.info('TIMESTAMP starting case event table load for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        ce_q = IncomingForm.select(IncomingForm.id, IncomingForm.form,
                                   IncomingForm.case, IncomingForm.alt_case,
                                   IncomingForm.closed, IncomingForm.created,
                                   IncomingForm.updated, IncomingForm.case_properties).distinct(([IncomingForm.form],
                                                                                                 [IncomingForm.case],
                                                                                                 [IncomingForm.alt_case])).where(
                                        (IncomingForm.domain == self.domain.name) 
                                        & (IncomingForm.record_type == 'case_event')
                                        & ((IncomingForm.imported == False) | (IncomingForm.imported >> None))).order_by(IncomingForm.id.desc())
                                   
        cur = CaseEvent._meta.database.execute_sql('select form.form_id, cases.case_id '
                                                'from form, cases, case_event '
                                                'where form.id = case_event.form_id '
                                                'and cases.id = case_event.case_id '
                                                'and form.domain_id = %d' % self.domain.id)
        existing_pairs = set(cur.fetchall())
        
        
        forms_cur = CaseEvent._meta.database.execute_sql('select form.form_id, form.id from form '
                                                'where form.domain_id = %d' % self.domain.id)
        forms_dict = dict(set(forms_cur.fetchall()))
        
        cases_cur = CaseEvent._meta.database.execute_sql('select cases.case_id, cases.id from cases '
                                                'where cases.domain_id = %d' % self.domain.id)
        cases_dict = dict(set(cases_cur.fetchall()))
        
        insert_dicts = []
        for ce in ce_q.iterator():
            ce.case = ce.case if ce.case else ce.alt_case
            created = (ce.created == "True")
            updated = (ce.updated == "True")
            closed = (ce.closed == "True")
            
            if ce.form in forms_dict and ce.case in cases_dict: 
                row = {'form':forms_dict[ce.form], 'case':cases_dict[ce.case], 
                       'closed': closed, 'created': created, 'updated':updated,
                       'case_properties': ce.case_properties}
                if (ce.form and ce.case) and ((ce.form, ce.case) not in existing_pairs):
                    insert_dicts.append(row)
                elif ce.case_properties:
                    q = CaseEvent.update(**row).where((CaseEvent.case == row['case']) & (CaseEvent.form == row['form']))
                    q.execute()
            else:
                logger.warning("while inserting case event, could not find either form %s or case %s in domain %s" 
                                 % (ce.form, ce.case if ce.case else ce.alt_case, self.domain.name))
                
        if insert_dicts:
            logger.info("inserting %d case events for domain %s" % (len(insert_dicts), self.domain.name))
            self.insert_chunked(insert_dicts)

class VisitLoader(Loader):
    '''
    loads data to the visit table from form data
    '''
    
    _db_warehouse_table_class = Visit

    def __init__(self, domain, regenerate_all):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        self.regenerate_all = regenerate_all
        super(VisitLoader, self).__init__()
         
    def delete_most_recent(self, user):
        vq = Visit.select().where(Visit.user == user).order_by(Visit.time_start.desc()).limit(1)
        if vq.count() > 0:
            v = vq.get()
            logger.debug('deleting most recent visit for user %s with id %d, start time %s' % (user.id, v.id, v.time_start))
            dq = Visit.delete().where(Visit.id == v.id)
            dq.execute()
        else:
            logger.debug('no visits to delete for user %s' % user.id)
            
    def delete_all(self, user):
        logger.debug('deleting all visits for user %s ' % user.user_id)
        dq = Visit.delete().where(Visit.user == user)
        dq.execute()
        
    def create_visit(self, user, visited_forms):
        
        time_start = min(visited_forms, key=lambda x : x.time_start).time_start
        time_end = max(visited_forms, key=lambda x : x.time_end).time_end
        v = Visit.create(user=user, time_start=time_start, time_end=time_end, domain=self.domain)
        
        ids = [f.id for f in visited_forms]
        uq = Form.update(visit=v).where(Form.id << ids)
        uq.execute()

        logger.debug('saved visit with id %d for user %s, %d forms' % (v.id, user.id, len(visited_forms)))
        
    def do_load(self):
        logger.info('TIMESTAMP starting visit table load for domain %s regenerate_all is %s' % (self.domain.name, self.regenerate_all))
        
        # all users who have submitted forms to this domain
        user_id_cur = Form._meta.database.execute_sql('select id from users where id in '
                                                    '(select user_id from form where domain_id = %d )' % self.domain.id)
        user_id_list = [item[0] for item in user_id_cur.fetchall()]
        
        if not user_id_list:
            logger.warn('No users to generate visits for in for domain %s ' % self.domain.name)
            return
        users = User.select().where(User.id << user_id_list).order_by(User.user_id)
        
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
            if self.regenerate_all:
                self.delete_all(usr)
            else:
                self.delete_most_recent(usr)
            
            logger.debug("getting visits for user %s" % usr.user_id)
            
            # forms already in visit
            prev_visited_forms = []
            # cases already in visit
            prev_visited_case_ids = []
            # parents of cases already in visit
            parents_of_prev_visited_cases = []
            # cases in the last form
            last_form_case_ids = []
            
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
                    
                    # if there is longer than 12 hours between the end time of the previous form and the start time of this one, don't add to visit
                    if (prev_visited_forms and (prev_visited_forms[len(prev_visited_forms) - 1].time_end < (frm.time_start - timedelta(hours=12)))):
                        add_to_previous = False
                    
                    # if all cases in this form are the same as all cases previously visited, add this form to the visit
                    elif set(form_case_ids) == set(last_form_case_ids):
                        prev_visited_forms.append(frm)
                        parents_of_prev_visited_cases.extend(form_case_parents)
                        add_to_previous = True
                        
                    # if cases in this form have parents already in this visit, add this form to the visit
                    elif len(set(form_case_ids) & set(parents_of_prev_visited_cases)) > 0:
                        prev_visited_forms.append(frm)
                        prev_visited_case_ids.extend(form_case_ids)
                        parents_of_prev_visited_cases.extend(form_case_parents)
                        last_form_case_ids = form_case_ids
                        add_to_previous = True
                    
                    # if cases in this form are parents of cases already in this visit, add this form to the visit
                    elif len(set(prev_visited_case_ids) & set(form_case_parents)) > 0:
                        prev_visited_forms.append(frm)
                        prev_visited_case_ids.extend(form_case_ids)
                        parents_of_prev_visited_cases.extend(form_case_parents)
                        last_form_case_ids = form_case_ids
                        add_to_previous = True
                        
                    else:
                        add_to_previous = False
            
                    # if we aren't adding this form to the previous visit save the previous visit
                    # create new lists of forms and cases for a new visit
                    if not add_to_previous:
                        if prev_visited_forms:
                            previous_visit = self.create_visit(usr, prev_visited_forms)
                            
                        prev_visited_case_ids = form_case_ids
                        parents_of_prev_visited_cases = form_case_parents
                        last_form_case_ids = form_case_ids
                        prev_visited_forms = [frm]
            
            # save the last visit for this user
            if prev_visited_forms:
                previous_visit = self.create_visit(usr, prev_visited_forms)
                
class DeviceLogLoader(Loader):
    '''
    loads data to the user table from the web-user API (incoming web users)
    '''
    
    _db_warehouse_table_class = DeviceLog
    
    def __init__(self, domain, user_loader):
        '''
        Constructor
        '''
        self.domain = Domain.get(name=domain)
        self.user_loader = user_loader
        super(DeviceLogLoader, self).__init__()
    
    def load_from_API(self):
        user_q = User.select()
        user_id_dict = dict([(u.user_id, u.id) for u in user_q])
    
        user_username_dict = dict([((u.username.split('@')[0]), u.id) for u in user_q if (u.username and '@' in u.username)])
        
        existing_cur = DeviceLog._meta.database.execute_sql('select api_id from device_log '
                                                            'where device_log.domain_id = %d' % self.domain.id)
        existing_log_ids = [d[0] for d in existing_cur.fetchall()]
        
        insert_dicts = []
        
        unimported_logs = IncomingDeviceLog.get_unimported(self.domain.name)
        logger.info('Incoming device log table has %d records not imported' % unimported_logs.count())
        
        for inc in unimported_logs.iterator():
            if inc.api_id not in existing_log_ids:
                try:
                    log_date = datetime.datetime.strptime(inc.log_date, '%Y-%m-%dT%H:%M:%S') if inc.log_date else None
                except ValueError, v:  # this is for log entries with decimal seconds 
                    # see http://stackoverflow.com/questions/5045210/how-to-remove-unconverted-data-from-a-python-datetime-object
                    if len(v.args) > 0 and v.args[0].startswith('unconverted data remains: '):
                        stripped_date = inc.log_date[:-(len(v.args[0]) - 26)]
                        log_date = datetime.datetime.strptime(stripped_date, '%Y-%m-%dT%H:%M:%S')
                    else:
                        raise v
                
                if (inc.user_id):
                    if inc.user_id in user_id_dict:
                        user_id = user_id_dict[inc.user_id]
                    elif inc.username and inc.username in user_username_dict:
                        user_id = user_username_dict[inc.username]
                    else:
                        logger.warn("while inserting device log for domain %s "
                        "couldn't find user with user ID %s or username %s" % (inc.domain, inc.user_id, inc.username))
                        new_id = self.user_loader.create_missing(inc.user_id)
                        user_id_dict[inc.user_id] = new_id
                else:
                    user_id = None
                        
                row = {'api_id':inc.api_id, 'domain':self.domain.id, 'app_version':inc.app_version,
                       'log_date':log_date, 'device_id':inc.device_id, 'form':inc.xform_id,
                       'i':inc.i, 'msg':inc.msg, 'resource_uri':inc.resource_uri, 'log_type':inc.log_type, 'user':user_id}
                insert_dicts.append(row)
    
        if insert_dicts:
            deduped = [dict(t) for t in set([tuple(d.items()) for d in insert_dicts])]
            logger.info("inserting %d device log entries for domain %s" % (len(deduped), self.domain.name))
            self.insert_chunked(deduped)
                

            
    def do_load(self):
        logger.info('TIMESTAMP starting device log table load for domain %s %s' % (self.domain.name, datetime.datetime.now()))
        self.load_from_API()
        
class SalesforceObjectLoader(Loader):
    
    def __init__(self):
        '''
        Constructor
        '''
        self.engine = sqlalchemy.create_engine(conf.SQLALCHEMY_DB_URL)
        super(SalesforceObjectLoader, self).__init__()
    
    def do_load(self):
        all_unimported = IncomingSalesforceRecord.get_unimported()
        object_types = all_unimported.select(IncomingSalesforceRecord.object_type).distinct()
        
        for obj in object_types:
            unimported_recs = all_unimported.select().where(IncomingSalesforceRecord.object_type == obj.object_type)
            unimported_dicts = [json.loads(rec.record) for rec in unimported_recs]
            for d in unimported_dicts:
                d['url'] = d['attributes']['url']
                del d['attributes']
                
                for k, v in d.iteritems():
                    if isinstance(v, dict):
                        d[k] = json.dumps(v)
                        
            df = DataFrame(unimported_dicts)
            df.columns = [colname.lower() for colname in df.columns]
            table_name = 'sf_%s' % (obj.object_type.lower())
            logger.info('Writing records for Salesforce object %s to db table %s' % (obj.object_type, table_name))
            df.to_sql(table_name, self.engine, flavor='postgresql', if_exists='replace', index=False, index_label=None)
            
            
