-- create application table
insert into application (app_name,app_id, domain_id) (select app_name, app_id, domain_id from formdef)

alter table formdef add foreign key (application_id) references parent(application);
alter table form add foreign key (application_id) references parent(application);
alter table form add foreign key (formdef_id) references parent(formdef);

update formdef set application_id = (select id from application where app_id like formdef.app_id)
update form set formdef_id = (select id from formdef where app_id = form.app_id and xmlns = form.xmlns)
update formdef set application_id = (select id from application where app_id like formdef.app_id)

alter table form drop column app_id
alter table form drop column xmlns

alter table formdef drop column app_name
alter table formdef drop column app_id

