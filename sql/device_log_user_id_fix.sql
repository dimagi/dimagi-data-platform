CREATE INDEX domain_name
  ON domain
  USING btree
  (name);

  CREATE INDEX incoming_device_log_domain
  ON incoming_device_log
  USING btree
  (domain);

CREATE INDEX users_user_id_domain_id
  ON users
  USING btree
  (user_id,domain_id);

--delete the users with no domain_id
update device_log set user_id = null where user_id in (select id from users where domain_id is null);
delete from users where domain_id is null;

--create new users
insert into users (user_id, domain_id)
(select incoming_device_log.user_id, domain.id from incoming_device_log, domain 
where incoming_device_log.domain = domain.name and not exists (select 1 from users where user_id = incoming_device_log.user_id and domain_id = domain.id)
and incoming_device_log.user_id is not null group by incoming_device_log.user_id, domain.id);

--link device logs to new users
update device_log set user_id = users.id from users, incoming_device_log
where incoming_device_log.api_id = device_log.api_id
and users.domain_id = device_log.domain_id
and incoming_device_log.user_id=users.user_id
and device_log.user_id is null
and incoming_device_log.user_id is not null