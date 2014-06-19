'''
Created on Jun 17, 2014

@author: mel
'''
'''
import logging
logger = logging.getLogger('peewee')
logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
'''

from peewee import PostgresqlDatabase, Model, CharField, DateTimeField, \
    ForeignKeyField, IntegerField, BooleanField, PrimaryKeyField, \
    drop_model_tables

from dimagi_data_platform import config


database = config.PEEWEE_DB_CON

class UnknownField(object):
    pass

class BaseModel(Model):
    class Meta:
        database = database
        
models = []

class User(BaseModel):
    id = PrimaryKeyField(db_column='id')
    user = CharField(db_column='user_id', max_length=255, null=True)
    domain = CharField(db_column='domain', max_length=255)

    class Meta:
        db_table = 'users'
models.append(User)

class Form(BaseModel):
    id = PrimaryKeyField(db_column='id')
    app = CharField(db_column='app_id', max_length=255, null=True)
    form = CharField(db_column='form_id', max_length=255)
    time_end = DateTimeField(null=True)
    time_start = DateTimeField(null=True)
    user = ForeignKeyField(db_column='user_id', null=True, rel_model=User, related_name='forms', on_delete='CASCADE')
    xmlns = CharField(max_length=255, null=True)
    domain = CharField(db_column='domain', max_length=255)

    class Meta:
        db_table = 'form'
models.append(Form)

class Cases(BaseModel):
    id = PrimaryKeyField(db_column='id')
    case = CharField(db_column='case_id', max_length=255)
    case_type = CharField(max_length=255, null=True)
    closed = CharField(max_length=255, null=True)
    date_closed = CharField(max_length=255, null=True)
    date_modified = CharField(max_length=255, null=True)
    date_opened = CharField(max_length=255, null=True)
    owner = ForeignKeyField(db_column='owner_id', null=True, rel_model=User, related_name='owned_cases', on_delete='CASCADE')
    parent = CharField(db_column='parent_id', max_length=255, null=True)
    user = ForeignKeyField(db_column='user_id', null=True, rel_model=User, related_name='user_cases', on_delete='CASCADE')
    domain = CharField(db_column='domain', max_length=255)

    class Meta:
        db_table = 'cases'
models.append(Cases)
        
class CaseEvent(BaseModel):
    id = PrimaryKeyField(db_column='id')
    case = ForeignKeyField(db_column='case_id', null=True, rel_model=Cases, related_name='caseevents', on_delete='CASCADE')
    form = ForeignKeyField(db_column='form_id', null=True, rel_model=Form, related_name='caseevents', on_delete='CASCADE')

    class Meta:
        db_table = 'case_event'
models.append(CaseEvent)

class Visit(BaseModel):
    form_duration = IntegerField(null=True)
    time_end = DateTimeField(null=True)
    time_start = DateTimeField(null=True)
    home_visit = BooleanField(null=True)
    user = ForeignKeyField(db_column='user_id', null=True, rel_model=User, related_name='visits', on_delete='CASCADE')
    visit = PrimaryKeyField(db_column='visit_id')

    class Meta:
        db_table = 'visit'
models.append(Visit)

class Interaction(BaseModel):
    case = ForeignKeyField(db_column='case_id', null=True, rel_model=Cases, related_name='interactions', on_delete='CASCADE')
    visit = ForeignKeyField(db_column='visit_id', null=True, rel_model=Visit, related_name='interactions', on_delete='CASCADE')

    class Meta:
        db_table = 'case_visit'
models.append(Interaction)

class FormVisit(BaseModel):
    form = ForeignKeyField(db_column='form_id', null=True, rel_model=Form, on_delete='CASCADE')
    visit = ForeignKeyField(db_column='visit_id', null=True, rel_model=Visit, related_name='form_visits', on_delete='CASCADE')

    class Meta:
        db_table = 'form_visit'
models.append(FormVisit)

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