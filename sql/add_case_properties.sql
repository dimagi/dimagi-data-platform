select attributes->'impact_case_properties' from domain where name like 'jharkhand-mch'

alter table incoming_form add column case_properties hstore;
alter table case_event add column case_properties hstore;

select form_id, case_id, case_properties 
from incoming_form where domain like 'jharkhand-mch' and record_type like 'case_event' 
order by id desc
limit 11071

select * from case_event 
where form_id in (select id from form where domain_id = (select id from domain where name like 'jharkhand-mch'))
and case_properties is not null