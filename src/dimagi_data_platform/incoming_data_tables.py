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
    class Meta:
        database = database
        
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

class IncomingCases(BaseModel):
    api = CharField(db_column='api_id', max_length=255, null=True)
    case = CharField(db_column='case_id', max_length=255, null=True)
    case_type = CharField(max_length=255, null=True)
    closed = CharField(max_length=255, null=True)
    date_closed = CharField(max_length=255, null=True)
    date_modified = CharField(max_length=255, null=True)
    date_opened = CharField(max_length=255, null=True)
    domain = CharField(max_length=255, null=True)
    owner = CharField(db_column='owner_id', max_length=255, null=True)
    parent = CharField(db_column='parent_id', max_length=255, null=True)
    user = CharField(db_column='user_id', max_length=255, null=True)

    class Meta:
        db_table = 'incoming_cases'
models.append(IncomingCases)

class IncomingForm(BaseModel):
    app = CharField(db_column='app_id', max_length=255, null=True)
    app_version = CharField(max_length=255, null=True)
    case = CharField(db_column='case_id', max_length=255, null=True)
    alt_case = CharField(db_column='alt_case_id', max_length=255, null=True)
    closed = CharField(max_length=255, null=True)
    created = CharField(max_length=255, null=True)
    device = CharField(db_column='device_id', max_length=255, null=True)
    domain = CharField(max_length=255, null=True)
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
