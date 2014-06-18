'''
Created on Jun 17, 2014

@author: mel
'''

from peewee import *

database = PostgresqlDatabase('data_platform_db_2', **{'host': 'localhost', 'password': 'notthis', 'user': 'importer'})

class UnknownField(object):
    pass

class BaseModel(Model):
    class Meta:
        database = database

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

class IncomingForm(BaseModel):
    app = CharField(db_column='app_id', max_length=255, null=True)
    app_version = CharField(max_length=255, null=True)
    case = CharField(db_column='case_id', max_length=255, null=True)
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

