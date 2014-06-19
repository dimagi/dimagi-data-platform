drop table if exists users;
drop table if exists form;
drop table if exists cases;
drop table if exists case_event;

CREATE TABLE users (
    id serial primary key,
    user_id character varying(255),
    domain character varying(255) NOT NULL
);

CREATE TABLE form (
    form_id character varying(255) primary key,
    app_id character varying(255),
    time_start timestamp without time zone,
    time_end timestamp without time zone,
    user_id int references users(id),
    xmlns character varying(255),
    domain character varying(255) NOT NULL
);

insert into users(user_id,domain) 
(select user_id, domain from incoming_cases union select user_id, domain from incoming_form union select owner_id, domain from incoming_cases);
delete from users where domain not like 'crs-remind';

insert into form (select distinct form_id,  app_id,  to_timestamp(replace(time_dtart,'T',' '),'YYYY-MM-DD HH24:MI:SS') as time_start,
to_timestamp(replace(time_end,'T',' '),'YYYY-MM-DD HH24:MI:SS') as time_end, users.id as user_id,  xmlns, incoming_form.domain
from incoming_form, users 
where users.user_id = incoming_form.user_id
group  by form_id, xmlns, app_id, time_start,time_end, users.id, incoming_form.domain);

CREATE TABLE cases (
    case_id character varying(255) primary key,
    case_type character varying(255),
    closed character varying(255),
    date_closed character varying(255),
    date_modified character varying(255),
    date_opened character varying(255),
    owner_id int references users(id),
    parent_id character varying(255),
    user_id int references users(id),
    domain character varying(255) NOT NULL
);

insert into cases (select case_id, case_type, closed, date_closed, date_modified, date_opened, b.id as owner_id, parent_id, a.id as user_id, incoming_cases.domain
from incoming_cases, users as a, users as b
where incoming_cases.domain like 'crs_remind'
and a.user_id = incoming_cases.user_id
and b.user_id = incoming_cases.owner_id
group by case_id, a.id, b.id, parent_id, case_type, date_opened, date_modified, closed, date_closed, incoming_cases.domain);

create table visit (visit_id serial primary key, 
user_id int references users(id),
time_start timestamp,
time_end timestamp,
form_duration int,
home_visit boolean);

CREATE TABLE case_event (
    id serial primary key,
    case_id character varying(255) references cases (case_id),
    form_id character varying(255) references form (form_id),
    visit_id int references visit(visit_id)
);

insert into case_event(form_id, case_id) (select form.form_id, cases.case_id 
from cases, form, incoming_form 
where incoming_form.domain like 'crs-remind' 
and incoming_form.case_id = cases.case_id
and incoming_form.form_id = form.form_id
group by form.form_id, cases.case_id);



with visit_windows as (select users.id as user_id, cases.case_id, coalesce (cases.parent_id, cases.case_id), form.form_id, time_start, time_end,
array_agg(case_event.id) over (partition by users.user_id, coalesce (cases.parent_id, cases.case_id)) as ce_arr
from form, users, case_event, cases
where form.form_id = case_event.form_id
and cases.case_id = case_event.case_id
and users.id = form.user_id
order by users.id,time_start),
visit_inserts as (insert into visit (user_id, time_start,time_end) (select user_id, min(time_start), max(time_end)
from visit_windows
group by visit_windows.user_id, visit_windows.ce_arr) returning visit_id)

update case_event set visit_id = visit_inserts.visit_id 
where case_event.id = a.ce_id







