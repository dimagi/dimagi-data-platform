alter table incoming_domain add column api_json json;
alter table incoming_domain alter column api_json drop not null;
alter table incoming_domain alter column attributes drop not null;

alter table domain add column countries text[];
update domain set countries = ARRAY[country];
alter table domain drop column country;

alter table domain add column billing_properties json;
alter table domain add column calculated_properties json;
alter table domain add column domain_properties json;
alter table domain rename column attributes to extra_attributes;
