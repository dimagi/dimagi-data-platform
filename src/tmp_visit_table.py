from peewee import *

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
    visit = ForeignKeyField(db_column='visit_id', null=True, rel_model=Visit, related_name='forms')

    class Meta:
        db_table = 'form_visit'
      
def create_visit(user, visited_forms, visited_cases, time_since_previous):
    
    v = Visit.create(user=user)

    for case_id in set(pvc.case for pvc in visited_cases):
        cs = Cases.get(Cases.case == case_id)
        i = Interaction.create(case=cs, visit=v)
        i.save()
    
    form_total_time = 0;
    for fp in visited_forms:
        fv = FormVisit.create(visit=v, form=fp)
        fv.save()
        
        if ((fp.time_end is not None) & (fp.time_start is not None)):  # ignore forms without start or end time
            form_total_time = form_total_time + (fp.time_end - fp.time_start).total_seconds()
    
    v.form_duration = form_total_time
    v.time_start = visited_forms[0].time_start
    v.time_end = visited_forms[len(visited_forms) - 1].time_end
    v.time_since_previous = time_since_previous
    
    return v

def annotate_home_visits():
    home_visit_forms = []
    for v in Visit.select():
        v.home_visit=False
        if len (set(home_visit_forms) & set (f.xmlns for f in v.forms)):
            v.home_visit=True
        v.save()
            
    
def annotate_batch_entry():
    for u in User.select().where(User.domain == 'crs-remind'):
        
        previous_visit = None
        for v in u.visits.order_by(Visit.time_start):
            if previous_visit:
                time_between = v.time_start - previous_visit.time_end
                print "calculated: %d saved: %d" % (time_between, v.time_since_previous)
                
            previous_visit = v
            
    
 
def create_visits():       
    for u in User.select().where(User.domain == 'crs-remind'):
        print("GETTING VISITS FOR USER %s" % u.user)
        prev_visited_forms = []
        prev_visited_cases = []
        time_since_previous = None
        
        for frm in u.forms.select().order_by(Form.time_start):
            case_events = frm.caseevents
            form_cases = [ce.case for ce in case_events]
            
            # check if this case is the same as or related to any of the cases already in the visit
            related_case_ids = [fc.parent for fc in form_cases if fc.parent]
            their_parents = [vc.parent for vc in prev_visited_cases if vc.parent]
            related_case_ids.extend(their_parents)
    
            # if the other cases in the visit are related to cases in this form, don't save yet, but add the form cases to the visit cases
            if (len(set([c.case for c in prev_visited_cases]) & set([c.case for c in form_cases])) > 0 | (len(set([c.case for c in prev_visited_cases]) & set(related_case_ids)) > 0)):
                prev_visited_cases.extend(form_cases)
                prev_visited_forms.append(frm)
    
            # otherwise save the previous visit and create new lists of forms and cases
            else:
                if prev_visited_forms:
                    previous_visit = create_visit(u, prev_visited_forms, prev_visited_cases, time_since_previous)
                    previous_visit.save()
                    print('saved visit %d with %d cases and %d forms') % (previous_visit.visit, previous_visit.interactions.select().count(), previous_visit.forms.select().count())
                    
                    if frm.time_start:
                        time_since_previous = (frm.time_start - previous_visit.time_end).total_seconds()
                    
                prev_visited_cases = form_cases
                prev_visited_forms = [frm]

create_visits()           
