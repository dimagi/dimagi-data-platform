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

      
def create_visit(user, visited_forms, visited_cases, time_since_previous):
    
    # check if there is already a visit for this user to any case in this visit within the last hour
    first_form_in_visit = min(visited_forms, key=lambda f: f.time_start)
    last_form_in_visit = max(visited_forms, key=lambda f: f.time_end)
    cutoff_time = first_form_in_visit.time_start - datetime.timedelta(minutes=60)
    most_recent_visits = Visit.select().where(Visit.time_end > cutoff_time)
    
    v = None
    for vis in most_recent_visits:
        if ((len(set([intr.case.case for intr in vis.interactions]) & set([visc.case for visc in visited_cases]))>0) # current cases overlap existing cases
        | (len(set([visc.parent for visc in visited_cases]) & set([intr.case.case for intr in vis.interactions])) >0) # current parents overlap existing cases
        | (len (set([intr.case.parent for intr in vis.interactions]) & set([visc.case for visc in visited_cases]))> 0)): # current cases overlap existing parents
            v = vis
            print 'there is already a visit for this case or a related case within an hour of this visit'
            break
    
    if not v:
        v = Visit.create(user=user)
    
    for case_id in set(pvc.case for pvc in visited_cases):
        cs = Cases.get(Cases.case == case_id)
        try:
            existing_interaction = Interaction.get(case=cs, visit=v)
        except Interaction.DoesNotExist:
            i = Interaction.create(case=cs, visit=v)
            i.save()
    
    form_total_time = 0;
    v.home_visit = v.home_visit if v.home_visit else False
    
    for fp in visited_forms:
        fv = FormVisit.create(visit=v, form=fp)
        fv.save()
        
        if fp.xmlns in home_visit_forms:
            v.home_visit = True
        
        if ((fp.time_end is not None) & (fp.time_start is not None)):  # ignore forms without start or end time
            form_total_time = form_total_time + (fp.time_end - fp.time_start).total_seconds()
    
    v.form_duration = form_total_time if not v.form_duration else v.form_duration + form_total_time
    v.time_start = v.time_start if v.time_start else first_form_in_visit.time_start
    v.time_end = last_form_in_visit.time_end
    v.time_since_previous = v.time_since_previous if v.time_since_previous else time_since_previous
    
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
    for u in User.select().where(User.domain == 'crs-remind'):
        print("GETTING VISITS FOR USER %s" % u.user)
        prev_visited_forms = []
        prev_visited_cases = []
        time_since_previous = None
        
        for frm in u.forms.select().order_by(Form.time_start):
            
            # we need to know when the last form already in this visit ended so that we know to only add this form if it's within a time cutoff
            last_form_end = max(prev_visited_forms, key=lambda f: f.time_end).time_end if len(prev_visited_forms)> 0 else frm.time_start
            
            # all the cases in this form
            case_events = frm.caseevents
            form_cases = [ce.case for ce in case_events]
            
            # parents of cases in this form
            related_case_ids = [fc.parent for fc in form_cases if fc.parent]
            # parents of cases already in the visit
            previously_visited_case_relateds = [vc.parent for vc in prev_visited_cases if vc.parent]
            
            # form case ids
            form_case_ids = [c.case for c in form_cases]
            # previously visited case ids
            prev_case_ids = [c.case for c in prev_visited_cases]
            
            # first, only consider adding this form to the current visit if it's within an hour of the end of the last form
            # then if there is overlap between cases already in this visit and cases in this form don't save yet, but add the form cases to the visit cases
            if (frm.time_start - last_form_end < (datetime.timedelta(minutes=60))) & (len(set(form_case_ids) & set(prev_case_ids)) > 0):
                prev_visited_cases.extend(form_cases)
                prev_visited_forms.append(frm)
            
            # first, only consider adding this form to the current visit if it's within an hour of the end of the last form
            # then if there is overlap between this form's related cases and cases already in this visit, or cases in this form and cases related to cases already in this visit
            elif (frm.time_start - last_form_end < (datetime.timedelta(minutes=60))) & ((len(set(prev_case_ids) & set(related_case_ids)) > 0) | 
                  (len(set(form_case_ids) & set(previously_visited_case_relateds)) > 0)):
                prev_visited_cases.extend(form_cases)
                prev_visited_forms.append(frm)
    
            # otherwise save the previous visit and create new lists of forms and cases for a new visit
            else:
                if prev_visited_forms:
                    previous_visit = create_visit(u, prev_visited_forms, prev_visited_cases, time_since_previous)
                    previous_visit.save()
                    print('saved visit %d with %d cases and %d forms') % (previous_visit.visit, previous_visit.interactions.select().count(), previous_visit.form_visits.select().count())
                    
                    if frm.time_start:
                        time_since_previous = (frm.time_start - previous_visit.time_end).total_seconds()
                    
                prev_visited_cases = form_cases
                prev_visited_forms = [frm]
        
        # save the last visit for this user
        previous_visit = create_visit(u, prev_visited_forms, prev_visited_cases, time_since_previous)
        previous_visit.save()

create_visits()
# annotate_home_visits()
# annotate_batch_entry()
