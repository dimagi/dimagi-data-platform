select form.id, form.form_id, time_start, visit_id, case_event.id, case_id
from form, case_event where domain_id in (select id from domain where name like 'crs-imci') and form.id = case_event.form_id order by time_start desc
select * from cases where id = 2197121
select * from case_event, form where domain_id in (select id from domain where name like 'melissa-test-project') and case_event.form_id = form.id order by form.time_start desc

select form_id, case_id, ccx_id from incoming_form where id = 7906453

alter table incoming_form add column ccx_id varchar(255);

select form.form_id, cases.case_id, case_event.closed, created, updated 
from form, case_event, cases where form.form_id like 'a2dd0d57-1d7b-4084-8fcf-5f3af348b976' and case_event.form_id = form.id and case_event.case_id=cases.id

select form.id, form.form_id, count(distinct case_event.id), count (distinct case_event.case_id)
from form, case_event
where case_event.form_id = form.id
group by form.id, form.form_id
having count(case_event.id) > 1

select case_event.form_id, case_event.case_id , count (case_event.id)
from case_event group by case_event.form_id, case_event.case_id having count (case_event.id) > 1
order by count (case_event.id) desc

select count(id) from case_event

-- add columnto incoming_form
alter table incoming_form add column ccx_id varchar;

--deduplicate case events
create table case_event_tmp as (select id, case_id, form_id from case_event group by id, case_id, form_id);
alter table case_event_tmp add CONSTRAINT case_event_pkey PRIMARY KEY (id);

alter table case_event_tmp add CONSTRAINT case_event_case_id_fkey FOREIGN KEY (case_id)
      REFERENCES cases (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE;
CREATE INDEX case_event_case_id_
  ON case_event_tmp
  USING btree
  (case_id);
  
alter table case_event_tmp add CONSTRAINT case_event_form_id_fkey FOREIGN KEY (form_id)
      REFERENCES form (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE;
CREATE INDEX case_event_form_id_
  ON case_event_tmp
  USING btree
  (form_id);

CREATE INDEX case_event_form_id_case_id
  ON case_event_tmp
  USING btree
  (form_id, case_id);
alter table case_event rename to case_event_old;
alter table case_event_tmp rename to case_event;
--add case activity columns
alter table case_event add column closed boolean;
alter table case_event add column created boolean;
alter table case_event add column updated boolean;
update case_event set closed = form.closed from form where case_event.form_id = form.id;
update case_event set created = form.created from form  where case_event.form_id = form.id;
update case_event set updated = form.updated from form where case_event.form_id = form.id;
alter table form drop column closed;
alter table form drop column created;
alter table form drop column updated;


-- correct form id
update form set form_id = split_part(form_id, '.', 1);
create table tmp_mapping as (select a.id as old_id, b.new_id, a.form_id from form a join (select max(id) as new_id, form_id from form group by form_id) b on a.form_id = b.form_id);
update case_event set form_id = tmp_mapping.new_id from tmp_mapping where case_event.form_id = tmp_mapping.old_id;

-- deduplicate form table
CREATE SEQUENCE case_event_id_seq;
SELECT setval('case_event_id_seq', (SELECT max(id) FROM case_event));
ALTER TABLE case_event ALTER COLUMN id SET DEFAULT nextval('case_event_id_seq'::regclass);

create index tmp_index on tmp_mapping using btree (new_id);
drop table case_event_old;
delete from form where id in (select old_id from tmp_mapping where new_id <> old_id);
-- set visits to null
update form set visit_id=null;
delete FROM visit WHERE NOT EXISTS (select 1 from form where form.visit_id = visit.id);

