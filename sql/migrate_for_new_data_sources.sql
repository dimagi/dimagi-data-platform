alter table formdef add column app_name character varying(255);
alter table formdef add column form_names hstore;
alter table formdef add column formdef_json json;

alter table users add column username character varying(255);
alter table users add column first_name character varying(255);
alter table users add column last_name character varying(255);
alter table users add column default_phone_number character varying(255);
alter table users add column email character varying(255);
alter table users add column groups character varying(255)[];
alter table users add column phone_numbers character varying(255)[];

alter table formdef alter column app_name set data type text;

alter table domain add column active boolean;
alter table domain add column test boolean;