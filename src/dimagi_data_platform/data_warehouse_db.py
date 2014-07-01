'''
Created on Jun 17, 2014

@author: mel
'''

import logging

from peewee import  Model, CharField, DateTimeField, \
    ForeignKeyField, IntegerField, BooleanField, PrimaryKeyField, \
    drop_model_tables
from playhouse.postgres_ext import HStoreField

from dimagi_data_platform import conf

logger = logging.getLogger('peewee')
logging.basicConfig(level=conf.log_level,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='/var/tmp/data_platform_run.log',
                    filemode='w')

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
    project_state =CharField(max_length=255, null=True)
    business_unit =CharField(max_length=255, null=True)
    
    attributes = HStoreField(null=True)
    class Meta:
        db_table = 'domain'
models.append(Domain)

class DomainSector(BaseModel):
    id = PrimaryKeyField(db_column='id')
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='domain-sectors')
    sector = ForeignKeyField(db_column='sector_id', null=True, rel_model=Sector, related_name='domain-sectors')

    class Meta:
        db_table = 'domain_sector'
models.append(DomainSector)

class DomainSubsector(BaseModel):
    id = PrimaryKeyField(db_column='id')
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='domain-subsectors')
    subsector = ForeignKeyField(db_column='subsector_id', null=True, rel_model=Subsector, related_name='domain-subsectors')

    class Meta:
        db_table = 'domain_subsector'
models.append(DomainSubsector)

class FormDefinition(BaseModel):
    id = PrimaryKeyField(db_column='id')
    xmlns = CharField(max_length=255, null=True)
    app_id = CharField(max_length=255, null=True)
    
    attributes = HStoreField(null=True)
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='formdefs')
    sector = ForeignKeyField(db_column='sector_id',  rel_model=Sector, related_name='formdefs', null=True)

    class Meta:
        db_table = 'formdef'
models.append(FormDefinition)

class FormDefinitionSubsector(BaseModel):
    id = PrimaryKeyField(db_column='id')
    formdef = ForeignKeyField(db_column='formdef_id', null=True, rel_model=Domain, related_name='formdef-subsectors')
    subsector = ForeignKeyField(db_column='subsector_id', null=True, rel_model=Subsector, related_name='formdef-subsectors')

    class Meta:
        db_table = 'formdef_subsector'
models.append(FormDefinitionSubsector)

class User(BaseModel):
    id = PrimaryKeyField(db_column='id')
    user = CharField(db_column='user_id', max_length=255, null=True)
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='users')

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
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='forms')

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
    domain = ForeignKeyField(db_column='domain_id', null=True, rel_model=Domain, related_name='cases')

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