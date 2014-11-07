select * from device_log where log_type like '%audio%' and domain_id= (select id from domain where name like 'tulasalud') limit 10;

select * from incoming_device_log where domain like 'tulasalud' and user_id is null and username is not null and log_type like 'audio';

select username, split_part(users.username, '@', 1) from users where domain_id = (select id from domain where name like 'tulasalud')

select log_type, count(id) from device_log where user_id is not null and domain_id= (select id from domain where name like 'tulasalud') group by log_type order by log_type; 
--we have user id for 261 113 device logs, beginning in 2011. 1051 of these are audio logs. 34 200 have a username
--we do not have user_id for 1 983 396 device logs, beginning in 2011. of these 33680 are audio logs

select count(id) from device_log where user_id is not null;
-- 12 500 637 have null user_id but not null username
-- 38 492 517 have null user_id
-- have not null user_id 7 409 604

CREATE INDEX incoming_device_log_api_id
  ON incoming_device_log
  USING btree (api_id);

CREATE INDEX device_log_api_id
  ON device_log
  USING btree (api_id);

CREATE INDEX users_user_id_index
  ON users
  USING btree
  (user_id);

CREATE INDEX users_username_index
  ON users
  USING btree
  (username);
  
CREATE INDEX incoming_device_log_user_id_index
  ON incoming_device_log
  USING btree
  (user_id);
  
CREATE INDEX incoming_device_log_username_index
  ON incoming_device_log
  USING btree
  (username);

alter table users add column username_trunc varchar(255);
update users set username_trunc = split_part(username, '@', 1) where username like '%@%';

CREATE INDEX users_username_trunc
  ON users
  USING btree
  (username_trunc);

update device_log set user_id = users.id 
from incoming_device_log, users, domain
where incoming_device_log.api_id = device_log.api_id 
and incoming_device_log.username = username_trunc
and incoming_device_log.username is not null
and incoming_device_log.user_id is null
and device_log.user_id is null
and incoming_device_log.domain = domain.name
and users.domain_id = domain.id;

alter table users drop column username_trunc;
  

