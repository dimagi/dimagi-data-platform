'''
Created on Jun 17, 2014

@author: mel
'''

import logging

from peewee import  Model, CharField, DateTimeField, \
    ForeignKeyField, IntegerField, BooleanField, PrimaryKeyField, \
    drop_model_tables, TextField
from playhouse.postgres_ext import HStoreField, ArrayField, JSONField

from dimagi_data_platform import conf


logger = logging.getLogger('peewee')

database = conf.PEEWEE_DB_CON

class UnknownField(object):
    pass

class BaseModel(Model):
    class Meta:
        database = database
        
models = []

class Sector(BaseModel):
    id = PrimaryKeyField(db_column='id')
    name = CharField(max_length=255, null=True)

    class Meta:
        db_table = 'sector'
models.append(Sector)

class Subsector(BaseModel):
    id = PrimaryKeyField(db_column='id')
    name = CharField(max_length=255, null=True)
    sector = ForeignKeyField(db_column='sector_id',  rel_model=Sector, related_name='subsectors', null=True)

    class Meta:
        db_table = 'subsector'
models.append(Subsector)

class Domain(BaseModel):
    id = PrimaryKeyField(db_column='id')
    name = CharField(max_length=255, null=True)
    organization = CharField(max_length=255, null=True)
    country = CharField(max_length=255, null=True)
    services = CharField(max_length=255, null=True)
    project_state = CharField(max_length=255, null=True)
    business_unit = CharField(max_length=255, null=True)
    active =  BooleanField(null=True)
    test =  BooleanField(null=True)
    last_hq_import = DateTimeField(null=True)
    
    attributes = HStoreField(null=True)
    class Meta:
        db_table = 'domain'
models.append(Domain)

class DomainSector(BaseModel):
    id = PrimaryKeyField(db_column='id')
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='domainsectors')
    sector = ForeignKeyField(db_column='sector_id', null=True, rel_model=Sector, related_name='domainsectors')

    class Meta:
        db_table = 'domain_sector'
models.append(DomainSector)

class DomainSubsector(BaseModel):
    id = PrimaryKeyField(db_column='id')
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='domainsubsectors')
    subsector = ForeignKeyField(db_column='subsector_id', null=True, rel_model=Subsector, related_name='domainsubsectors')

    class Meta:
        db_table = 'domain_subsector'
models.append(DomainSubsector)

class Application(BaseModel):
    app_id = CharField(max_length=255, null=True)
    app_name = TextField(db_column='app_name', null=True)
    
    app_json = JSONField(db_column='app_json', null=True)
    attributes = HStoreField(null=True)
    
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='apps')
    
    @staticmethod
    def get_by_app_id_str(app_id_str, domain):
        if not app_id_str:
            return None
        else:
            try:
                app = Application.get(Application.app_id==app_id_str, Application.domain==domain)
            except Application.DoesNotExist:
                app = None
        return app
    
    class Meta:
        db_table = 'application'
models.append(Application)

class ApplicationSector(BaseModel):
    id = PrimaryKeyField(db_column='id')
    application = ForeignKeyField(db_column='app_id', null=True, rel_model=Application, related_name='appsectors')
    sector = ForeignKeyField(db_column='sector_id', null=True, rel_model=Sector, related_name='appsectors')

    class Meta:
        db_table = 'application_sector'
models.append(ApplicationSector)

class ApplicationSubsector(BaseModel):
    id = PrimaryKeyField(db_column='id')
    application = ForeignKeyField(db_column='app_id', null=True, rel_model=Application, related_name='appsubsectors')
    subsector = ForeignKeyField(db_column='subsector_id', null=True, rel_model=Subsector, related_name='appsubsectors')

    class Meta:
        db_table = 'application_subsector'
models.append(ApplicationSubsector)

class FormDefinition(BaseModel):
    id = PrimaryKeyField(db_column='id')
    xmlns = CharField(max_length=255, null=True)

    form_names = HStoreField(db_column='form_names', null=True)
    formdef_json = JSONField(db_column='formdef_json', null=True)
    
    attributes = HStoreField(null=True)
    application = ForeignKeyField(db_column='application_id', null=True, rel_model=Application, related_name='formdefs')
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='formdefs')
    sector = ForeignKeyField(db_column='sector_id',  rel_model=Sector, related_name='formdefs', null=True)
    
    @staticmethod
    def get_by_xmlns_and_application(xmlns,application, domain):
        if not xmlns:
            return None
        else:
            try:
                if not application:
                    fd = FormDefinition.get(FormDefinition.xmlns==xmlns,FormDefinition.application>>None, FormDefinition.domain==domain)
                else:
                    fd = FormDefinition.get(FormDefinition.xmlns==xmlns,FormDefinition.application==application, FormDefinition.domain==domain)
            except FormDefinition.DoesNotExist:
                fd = None
        return fd

    class Meta:
        db_table = 'formdef'
models.append(FormDefinition)

class FormDefinitionSubsector(BaseModel):
    id = PrimaryKeyField(db_column='id')
    formdef = ForeignKeyField(db_column='formdef_id', null=True, rel_model=Domain, related_name='formdefsubsectors')
    subsector = ForeignKeyField(db_column='subsector_id', null=True, rel_model=Subsector, related_name='formdefsubsectors')

    class Meta:
        db_table = 'formdef_subsector'
models.append(FormDefinitionSubsector)

class User(BaseModel):
    id = PrimaryKeyField(db_column='id')
    user = CharField(db_column='user_id', max_length=255, null=True)
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='users')
    
    username = CharField(db_column='username', max_length=255, null=True)
    first_name = CharField(db_column='first_name', max_length=255, null=True)
    last_name = CharField(db_column='last_name', max_length=255, null=True)
    default_phone_number = CharField(db_column='default_phone_number', max_length=255, null=True)
    email = CharField(db_column='email', max_length=255, null=True)
    groups = ArrayField(CharField,null=True)
    phone_numbers= ArrayField(CharField,null=True)

    class Meta:
        db_table = 'users'
models.append(User)

class WebUser(BaseModel):
    id = PrimaryKeyField(db_column='id')
    user = CharField(db_column='user_id', max_length=255, null=True)
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='webusers')
    username = CharField(db_column='username', max_length=255, null=True)
    first_name = CharField(db_column='first_name', max_length=255, null=True)
    last_name = CharField(db_column='last_name', max_length=255, null=True)
    default_phone_number = CharField(db_column='default_phone_number', max_length=255, null=True)
    email = CharField(db_column='email', max_length=255, null=True)
    is_admin = BooleanField(db_column='is_admin', null=True)
    resource_uri = CharField(db_column='resource_uri', null=True)
    webuser_role = CharField(db_column='webuser_role',max_length=255, null=True)
    phone_numbers= ArrayField(CharField,null=True)

    class Meta:
        db_table = 'web_user'
models.append(WebUser)

class Visit(BaseModel):
    id = PrimaryKeyField(db_column='id')
    time_end = DateTimeField(null=True)
    time_start = DateTimeField(null=True)

    user = ForeignKeyField(db_column='user_id', null=True, rel_model=User, related_name='visits', on_delete='CASCADE')
    class Meta:
        db_table = 'visit'
models.append(Visit)

class Form(BaseModel):
    id = PrimaryKeyField(db_column='id')

    form = CharField(db_column='form_id', max_length=255)
    time_end = DateTimeField(null=True)
    time_start = DateTimeField(null=True)
    user = ForeignKeyField(db_column='user_id', null=True, rel_model=User, related_name='forms', on_delete='CASCADE')
    
    app_version = CharField(max_length=255, null=True)
 
    device = CharField(db_column='device_id', max_length=255, null=True)
    is_phone_submission = BooleanField(null=True)
    received_on = DateTimeField(null=True)
    
    application = ForeignKeyField(db_column='application_id', null=True, rel_model=Application, related_name='forms', on_delete='SET NULL')
    formdef = ForeignKeyField(db_column='formdef_id', null=True, rel_model=FormDefinition, related_name='forms', on_delete='SET NULL')
    visit = ForeignKeyField(db_column='visit_id', null=True, rel_model=Visit, related_name='forms', on_delete='SET NULL')
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='forms')

    class Meta:
        db_table = 'form'
models.append(Form)

class Cases(BaseModel):
    id = PrimaryKeyField(db_column='id')
    case = CharField(db_column='case_id', max_length=255)
    case_type = CharField(max_length=255, null=True)
    closed = BooleanField(null=True)
    date_closed = DateTimeField(null=True)
    date_modified = DateTimeField(null=True)
    date_opened = DateTimeField(null=True)
    
    owner = ForeignKeyField(db_column='owner_id', null=True, rel_model=User, related_name='owned_cases', on_delete='CASCADE')
    parent = CharField(db_column='parent_id', max_length=255, null=True) #TODO: implement related cases
    user = ForeignKeyField(db_column='user_id', null=True, rel_model=User, related_name='user_cases', on_delete='CASCADE')
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='cases')

    class Meta:
        db_table = 'cases'
models.append(Cases)
        
class CaseEvent(BaseModel):
    id = PrimaryKeyField(db_column='id')
    closed = BooleanField(null=True)
    created = BooleanField(null=True)
    updated = BooleanField(null=True)
    case = ForeignKeyField(db_column='case_id', null=True, rel_model=Cases, related_name='caseevents', on_delete='CASCADE')
    form = ForeignKeyField(db_column='form_id', null=True, rel_model=Form, related_name='caseevents', on_delete='CASCADE')

    class Meta:
        db_table = 'case_event'
models.append(CaseEvent)

class DeviceLog(BaseModel):
    app_version = CharField(db_column='app_version', max_length=255, null=True)
    log_date = DateTimeField(db_column='log_date', null=True)
    device_id = CharField(db_column='device_id', max_length=255, null=True)
    i = IntegerField(db_column='i', null=True)
    api_id = IntegerField(db_column='api_id', null=True)
    msg = TextField(db_column='msg', null=True)
    resource_uri= CharField(db_column='resource_uri',  null=True)
    log_type = CharField(db_column='log_type', null=True)
    form = CharField(db_column='form_id', max_length=255, null=True) # this is the device log xform, not the form the user is submitting
    
    user = ForeignKeyField(db_column='user_id',  null=True, rel_model=User, related_name='device_logs')
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='device_logs')
    
    class Meta:
        db_table = 'device_log'
models.append(DeviceLog)

class HQExtractLog(BaseModel):
    extractor = CharField(db_column='extractor', max_length=255)
    extract_start = DateTimeField(null=True) # null start means we extracted records since forever
    extract_end = DateTimeField() # extract end cannot be null, it is either the last specified extract until date, or the current date
    
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='hq_extract_logs')
    
    @classmethod
    def get_last_extract_log(cls,extractor_name, domain):
        q = HQExtractLog.select().where((HQExtractLog.extractor == extractor_name) & (HQExtractLog.domain == domain)).order_by(HQExtractLog.extract_end.desc())
        return q.get()
    
    class Meta:
        db_table = 'hq_extract_log'
models.append(HQExtractLog)

def create_missing_tables():
    database.connect()
    
    for m in models:
        m.create_table(fail_silently=True)
        
    database.commit()
        
def drop_and_create():
    database.connect()
    drop_model_tables(models,fail_silently=True)
    
    for m in models:
        m.create_table()
        
    database.commit()