from peewee import *
import datetime

database = PostgresqlDatabase('data_platform_db', **{'host': 'localhost', 'password': 'notthis', 'user': 'importer'})

class UnknownField(object):
    pass

class BaseModel(Model):
    class Meta:
        database = database

class User(BaseModel):
    user = CharField(db_column='user_id', max_length=255, null=True)
    domain = CharField(db_column='domain', max_length=255)

    class Meta:
        db_table = 'users'

class Form(BaseModel):
    app = CharField(db_column='app_id', max_length=255, null=True)
    form = CharField(db_column='form_id', max_length=255, primary_key=True)
    time_end = DateTimeField(null=True)
    time_start = DateTimeField(null=True)
    user = ForeignKeyField(db_column='user_id', null=True, rel_model=User, related_name='forms')
    xmlns = CharField(max_length=255, null=True)

    class Meta:
        db_table = 'form'

class Cases(BaseModel):
    case = CharField(db_column='case_id', max_length=255, primary_key=True)
    case_type = CharField(max_length=255, null=True)
    closed = CharField(max_length=255, null=True)
    date_closed = CharField(max_length=255, null=True)
    date_modified = CharField(max_length=255, null=True)
    date_opened = CharField(max_length=255, null=True)
    owner = ForeignKeyField(db_column='owner_id', null=True, rel_model=User, related_name='owned_cases')
    parent = CharField(db_column='parent_id', max_length=255, null=True)
    user = ForeignKeyField(db_column='user_id', null=True, rel_model=User, related_name='user_cases')

    class Meta:
        db_table = 'cases'
        
class CaseEvent(BaseModel):
    case = ForeignKeyField(db_column='case_id', null=True, rel_model=Cases, related_name='form-occurences')
    form = ForeignKeyField(db_column='form_id', null=True, rel_model=Form, related_name='caseevents')

    class Meta:
        db_table = 'case_event'

class Visit(BaseModel):
    batch_entry = BooleanField(null=True)
    form_duration = IntegerField(null=True)
    home_visit = BooleanField(null=True)
    time_end = DateTimeField(null=True)
    time_since_previous = IntegerField(null=True)
    time_start = DateTimeField(null=True)
    user = ForeignKeyField(db_column='user_id', null=True, rel_model=User, related_name='visits')
    visit = PrimaryKeyField(db_column='visit_id')

    class Meta:
        db_table = 'visit'


class Interaction(BaseModel):
    case = ForeignKeyField(db_column='case_id', null=True, rel_model=Cases, related_name='interactions')
    visit = ForeignKeyField(db_column='visit_id', null=True, rel_model=Visit, related_name='interactions')

    class Meta:
        db_table = 'case_visit'

class FormVisit(BaseModel):
    form = ForeignKeyField(db_column='form_id', null=True, rel_model=Form)
    visit = ForeignKeyField(db_column='visit_id', null=True, rel_model=Visit, related_name='form_visits')

    class Meta:
        db_table = 'form_visit'
        
home_visit_forms = set(['http://openrosa.org/formdesigner/E41E21BB-32F9-435B-A3E9-BEB08C4743A1',
'http://openrosa.org/formdesigner/0FA17225-70BC-4284-A38B-6C4E16E6CA4F',
'http://openrosa.org/formdesigner/4A49479F-BEBD-498D-A08B-4F0EAAD4DDBB',
'http://openrosa.org/formdesigner/FDD31334-4885-4C31-AEA4-59547CCC5C9E',
'http://openrosa.org/formdesigner/93BC80C0-E9EC-4A0E-BDE0-ACE69CB30B88',
'http://openrosa.org/formdesigner/63866D7C-42FC-43DD-8EFA-E02C74729DD6',
'http://openrosa.org/formdesigner/5314838B-00ED-4556-A656-100EAFD0603F',
'http://openrosa.org/formdesigner/EF241BF0-6230-4390-A8BE-31803E7A135E',
'http://openrosa.org/formdesigner/a6fafcf2cc3e280533aa0325b513d73f3c41c4ef',
'http://openrosa.org/formdesigner/AC501474-0BA9-4915-9A8C-A8A8F6C57BFF',
'http://openrosa.org/formdesigner/EA8FB6FC-E269-440F-993E-AD07F733BF31',
'http://openrosa.org/formdesigner/fa8deed6f9b7c955a077e987a919e2815cf2afa5',
'http://openrosa.org/formdesigner/36c07ca15eb402fb6ba3cedd709c6ce7e1e83685'])

      
def create_visit(user, visited_forms, visited_cases):
    
    v = Visit.create(user=user)
    
    for cs in visited_cases:
        i = Interaction.create(case=cs, visit=v)
        i.save()
        
    v.home_visit = v.home_visit if v.home_visit else False
    
    for fp in visited_forms:
        fv = FormVisit.create(visit=v, form=fp)
        fv.save()
        
        if fp.xmlns in home_visit_forms:
            v.home_visit = True
 
    return v
    
def annotate_batch_entry():
    for u in User.select().where(User.domain == 'crs-remind'):
        
        previous_visit = None
        for v in u.visits.order_by(Visit.time_start):
            if previous_visit:
                time_between = v.time_start - previous_visit.time_end
                print "this visit %d previous visit %d calculated: %d saved: %d" % (v.visit, previous_visit.visit, time_between.total_seconds(), v.time_since_previous)
                
            previous_visit = v
            
    
 
def create_visits():
    case_parents = Cases.select().where(Cases.parent != None)
    case_parent_dict = {cp.case: cp.parent for cp in case_parents} 
    
    for u in User.select().where(User.domain == 'crs-remind').where(User.user=='30ed810aebe033d01c01037b35914daa'):
        print("GETTING VISITS FOR USER %s" % u.user)
        prev_visited_forms = []
        prev_visited_case_ids = []
        
        for frm in u.forms.select().order_by(Form.time_start):
            
            # all the cases in this form
            case_events = frm.caseevents
            
            if case_events.count() > 0:
                form_case_ids = [ce.case.case for ce in case_events]
                print form_case_ids
                
                # parents of cases in this form
                related_case_ids = [case_parent_dict[fc] for fc in form_case_ids if fc in case_parent_dict]
                # parents of cases already in the visit
                previously_visited_case_relateds = [case_parent_dict[vc] for vc in prev_visited_case_ids if vc in case_parent_dict]
                
                # if there is overlap between cases already in this visit and cases in this form don't save yet, but add the form cases to the visit cases
                if len(set(form_case_ids) & set(prev_visited_case_ids)) > 0:
                    prev_visited_case_ids.extend(form_case_ids)
                    prev_visited_forms.append(frm)
                
                # if there is overlap between this form's related cases and cases already in this visit, or cases in this form and cases related to cases already in this visit
                elif (len(set(prev_visited_case_ids) & set(related_case_ids)) > 0) | (len(set(form_case_ids) & set(previously_visited_case_relateds)) > 0):
                    prev_visited_case_ids.extend(form_case_ids)
                    prev_visited_forms.append(frm)
    
        
                # otherwise save the previous visit and create new lists of forms and cases for a new visit
                else:
                    if prev_visited_forms:
                        prev_visited_cases = Cases.select().where(Cases.case << prev_visited_case_ids)
                        previous_visit = create_visit(u, prev_visited_forms, prev_visited_cases)
                        previous_visit.save()
                        print('saved visit %d with %d cases and %d forms') % (previous_visit.visit, previous_visit.interactions.select().count(), previous_visit.form_visits.select().count())
                        
                    prev_visited_case_ids = form_case_ids
                    prev_visited_forms = [frm]
        
        # save the last visit for this user
        prev_visited_cases = Cases.select().where(Cases.case << prev_visited_case_ids)
        previous_visit = create_visit(u, prev_visited_forms, prev_visited_cases)
        previous_visit.save()

create_visits()
# annotate_home_visits()
# annotate_batch_entry()
