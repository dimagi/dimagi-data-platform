from playhouse.migrate import *

from dimagi_data_platform import conf, data_warehouse_db


db = data_warehouse_db.database
migrator = PostgresqlMigrator(conf.PEEWEE_DB_CON)


def main ():
    
    # copy web user data into user table
    db.execute_sql('update users set username=web_user.username, first_name=web_user.first_name, last_name=web_user.last_name,'
                   ' default_phone_number = web_user.default_phone_number, email = web_user.email, phone_numbers=web_user.phone_numbers '
                   ' from web_user where users.user_id = web_user.user_id;')
    
    db.execute_sql('alter table web_user rename to web_user_old;')
    
    data_warehouse_db.create_missing_tables()
    
    db.execute_sql('insert into mobile_user (user_pk, domain_id) (select id, domain_id from users where user_id not in (select user_id from web_user_old));')
    db.execute_sql('insert into web_user (user_pk)'
                   ' (select users.id'
                   ' from users, web_user_old where users.user_id = web_user_old.user_id '
                   ' group by users.id);')
    db.execute_sql('insert into web_user_domain (web_user_pk, domain_id, resource_uri,webuser_role, is_admin)'
                   ' (select users.id, domain.id, web_user_old.resource_uri, web_user_old.webuser_role, web_user_old.is_admin'
                   ' from users, web_user_old, domain '
                   ' where users.user_id = web_user_old.user_id and domain.id = web_user_old.domain_id);')
    
    
    migrate(migrator.drop_column('users', 'groups'),)
    migrate(migrator.drop_column('users', 'domain_id'),)
    
    db.execute_sql('drop table web_user_old;')


if __name__ == '__main__':
    main()