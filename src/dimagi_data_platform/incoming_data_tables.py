'''
Created on Jun 23, 2014

@author: mel
'''


from datetime import datetime
import logging

from peewee import Model, CharField, drop_model_tables, PrimaryKeyField
from playhouse.postgres_ext import *

from dimagi_data_platform import conf


logger = logging.getLogger('peewee')

database = conf.PEEWEE_DB_CON

class UnknownField(object):
    pass

class BaseModel(Model):
    id = PrimaryKeyField(db_column='id')
    imported = BooleanField(default=False, null=True)
    
    @classmethod
    def get_unimported(cls):
        return cls.select().where((cls.imported == False) | (cls.imported >> None))
    
    class Meta:
        database = database
        
class BaseDomainLinkedModel(BaseModel):
    domain = CharField(max_length=255, null=True)
    
    @classmethod
    def get_unimported(cls, dname):
        return cls.select().where((cls.domain == dname) & ((cls.imported == False) | (cls.imported >> None)))
        
models = []

class IncomingDomain(BaseModel):
    
    attributes = HStoreField()

    class Meta:
        db_table = 'incoming_domain'
models.append(IncomingDomain)
        
class IncomingDomainAnnotation(BaseModel):
    attributes = HStoreField()

    class Meta:
        db_table = 'incoming_domain_annotation'
models.append(IncomingDomainAnnotation)
        
class IncomingFormAnnotation(BaseModel):
    attributes = HStoreField()

    class Meta:
        db_table = 'incoming_form_annotation'
models.append(IncomingFormAnnotation)

class IncomingCases(BaseDomainLinkedModel):
    api = CharField(db_column='api_id', max_length=255, null=True)
    case = CharField(db_column='case_id', max_length=255, null=True)
    case_type = CharField(max_length=255, null=True)
    closed = CharField(max_length=255, null=True)
    date_closed = CharField(max_length=255, null=True)
    date_modified = CharField(max_length=255, null=True)
    date_opened = CharField(max_length=255, null=True)
    
    owner = CharField(db_column='owner_id', max_length=255, null=True)
    parent = CharField(db_column='parent_id', max_length=255, null=True)
    user = CharField(db_column='user_id', max_length=255, null=True)

    class Meta:
        db_table = 'incoming_cases'
models.append(IncomingCases)

class IncomingForm(BaseDomainLinkedModel):
    app = CharField(db_column='app_id', max_length=255, null=True)
    app_version = CharField(max_length=255, null=True)
    case = CharField(db_column='case_id', max_length=255, null=True)
    alt_case = CharField(db_column='alt_case_id', max_length=255, null=True)
    closed = CharField(max_length=255, null=True)
    created = CharField(max_length=255, null=True)
    device = CharField(db_column='device_id', max_length=255, null=True)
    
    form = CharField(db_column='form_id', max_length=255, null=True)
    is_phone_submission = CharField(max_length=255, null=True)
    received_on = CharField(max_length=255, null=True)
    time_end = CharField(max_length=255, null=True)
    time_start = CharField(max_length=255, null=True)
    updated = CharField(max_length=255, null=True)
    user = CharField(db_column='user_id', max_length=255, null=True)
    xmlns = CharField(max_length=255, null=True)

    class Meta:
        db_table = 'incoming_form'
models.append(IncomingForm)

class IncomingUser(BaseDomainLinkedModel):
    user_id = CharField(db_column='user_id', max_length=255, null=True)
    username = CharField(db_column='username', max_length=255, null=True)
    first_name = CharField(db_column='first_name', max_length=255, null=True)
    last_name = CharField(db_column='last_name', max_length=255, null=True)
    default_phone_number = CharField(db_column='default_phone_number', max_length=255, null=True)
    email = CharField(db_column='email', max_length=255, null=True)
    groups = CharField(db_column='groups', null=True)
    phone_numbers= CharField(db_column='phone_numbers',  null=True)
    user_data = CharField(db_column='user_data',  null=True)
    
    class Meta:
        db_table = 'incoming_users'
models.append(IncomingUser)

class IncomingDeviceLog(BaseDomainLinkedModel):
    app_version = CharField(db_column='app_version', max_length=255, null=True)
    log_date = CharField(db_column='log_date', max_length=255, null=True)
    device_id = CharField(db_column='device_id', max_length=255, null=True)
    i = IntegerField(db_column='i', null=True)
    api_id = IntegerField(db_column='api_id', null=True)
    msg = TextField(db_column='msg', null=True)
    resource_uri= CharField(db_column='resource_uri',  null=True)
    log_type = CharField(db_column='log_type', null=True)
    user_id = CharField(db_column='user_id',  null=True)
    username = CharField(db_column='username',  null=True)
    xform_id = CharField(db_column='xform_id',  null=True)
    
    class Meta:
        db_table = 'incoming_device_log'
models.append(IncomingDeviceLog)

class IncomingWebUser(BaseDomainLinkedModel):
    api_id = CharField(db_column='api_id', max_length=255, null=True)
    username = CharField(db_column='username', max_length=255, null=True)
    first_name = CharField(db_column='first_name', max_length=255, null=True)
    last_name = CharField(db_column='last_name', max_length=255, null=True)
    default_phone_number = CharField(db_column='default_phone_number', max_length=255, null=True)
    email = CharField(db_column='email', max_length=255, null=True)
    is_admin = BooleanField(db_column='is_admin', null=True)
    resource_uri = CharField(db_column='resource_uri', null=True)
    webuser_role = CharField(db_column='webuser_role', null=True)
    phone_numbers= CharField(db_column='phone_numbers',  null=True)
    
    class Meta:
        db_table = 'incoming_web_user'
models.append(IncomingWebUser)

class IncomingFormDef(BaseDomainLinkedModel):
    app_id = CharField(db_column='app_id', max_length=255, null=True)
    app_name = CharField(db_column='app_name', max_length=255, null=True)
    form_names = CharField(db_column='form_names', null=True)
    form_xmlns = CharField(db_column='form_xmlns', max_length=255, null=True)
    formdef_json = TextField(db_column='formdef_json', null=True)

    class Meta:
        db_table = 'incoming_formdef'
models.append(IncomingFormDef)
        
def create_missing_tables():
    database.connect()
    
    for m in models:
        m.create_table(fail_silently=True)
        
    database.commit()
        
def drop_and_create():
    database.connect()
    drop_model_tables(models, fail_silently=True)
    
    for m in models:
        m.create_table()
        
    database.commit()
