alter table web_user rename to web_user_old;

update users set username = null where user_id in (select user_id from web_user_old);

update users set username=web_user_old.username, first_name=web_user_old.first_name, last_name=web_user_old.last_name,
default_phone_number = web_user_old.default_phone_number, email = web_user_old.email, phone_numbers=web_user_old.phone_numbers 
from web_user_old where users.user_id = web_user_old.user_id and users.id in (select max (id) from users group by user_id);

drop table tmp_user_mapping;
create table tmp_user_mapping as 
(select a.user_id, a.id as new_id, b.id as old_id, b.domain_id as old_domain_id
from (select id, user_id from users where username is not null or id in (select max(id) from users where username is null group by user_id)
 or id = (select max(id) from users where user_id is null)) as a, users as b
where ((a.user_id = b.user_id) or (a.user_id is null and b.user_id is null)));
select * from tmp_user_mapping;
select user_id, count(old_id) from tmp_user_mapping where old_id <> new_id group by user_id;
select user_id, count (distinct new_id) from tmp_user_mapping group by user_id;

update form set user_id = new_id from tmp_user_mapping where form.user_id = old_id and old_id <> new_id;
update cases set user_id = new_id from tmp_user_mapping where cases.user_id = old_id and old_id <> new_id;
update cases set owner_id = new_id from tmp_user_mapping where cases.owner_id = old_id and old_id <> new_id;
update device_log set user_id = new_id from tmp_user_mapping where device_log.user_id = old_id and old_id <> new_id;

--create missing tables
delete from mobile_user_domain;
delete from mobile_user;
insert into mobile_user (select id, groups from users where user_id not in (select user_id from web_user_old) and id in (select new_id from tmp_user_mapping));
insert into mobile_user_domain(mobile_user_pk, domain_id) (select new_id, old_domain_id from tmp_user_mapping where user_id not in (select user_id from web_user_old));

delete from web_user_domain;
delete from web_user;
insert into web_user (select id from users where user_id in (select user_id from web_user_old) and id in (select new_id from tmp_user_mapping));
insert into web_user_domain(web_user_pk, domain_id, is_admin, webuser_role, resource_uri) (select new_id, old_domain_id, is_admin, webuser_role, resource_uri 
from tmp_user_mapping, web_user_old where web_user_old.user_id = tmp_user_mapping.user_id);

delete from users where id not in (select new_id from tmp_user_mapping)

select user_pk, domain_id 
from mobile_user, users 
where users.id = mobile_user.user_pk and users.username is not null


select user_id, username, count(id) from users  group by user_id,username having count (id) > 1