from playhouse.migrate import *

from dimagi_data_platform import conf, data_warehouse_db


db = data_warehouse_db.database
migrator = PostgresqlMigrator(conf.PEEWEE_DB_CON)


def main ():

    data_warehouse_db.create_missing_tables()
    #deduplicate
    db.execute_sql('delete from formdef where id not in (select max(id) from formdef group by app_id, xmlns, domain_id);')
    db.execute_sql('insert into application (app_name,app_id, domain_id) (select app_name, app_id, domain_id from formdef group by app_name, app_id, domain_id);')
    
    # peewee migrations can't do constraints currently?
    migrate(migrator.add_column('formdef', 'application_id', data_warehouse_db.FormDefinition.application),)
    db.execute_sql('alter table formdef add foreign key (application_id) references application(id);')
 
    db.execute_sql('create index formdef_application_id ON formdef using btree (application_id);')
    
    migrate(migrator.add_column('form', 'application_id', data_warehouse_db.Form.application),)
    db.execute_sql('alter table form add foreign key (application_id) references application(id);')
    db.execute_sql('create index form_application_id ON form using btree (application_id);')
    
    migrate(migrator.add_column('form', 'formdef_id', data_warehouse_db.Form.formdef),)
    db.execute_sql('alter table form add foreign key (formdef_id) references formdef(id);')
    db.execute_sql('create index form_formdef_id ON form using btree (formdef_id);')
    

    db.execute_sql('update formdef set application_id = sub.id from  (select distinct id, app_id from application) as sub where sub.app_id like formdef.app_id;')
    db.execute_sql('update form set formdef_id = sub.id from (select distinct id, app_id, xmlns, domain_id from formdef) as sub where sub.app_id = form.app_id and sub.xmlns = form.xmlns and sub.domain_id = form.domain_id;')
    db.execute_sql('update form set application_id = sub.id from (select distinct id, app_id, domain_id from application) as sub where sub.app_id = form.app_id and sub.domain_id = form.domain_id;')
    db.execute_sql('update formdef set application_id = sub.id from (select distinct id, app_id from application) as sub where sub.app_id like formdef.app_id;')
    
    
    migrate(migrator.drop_column('form', 'app_id'),)
    migrate(migrator.drop_column('form', 'xmlns'),)
    migrate(migrator.drop_column('formdef', 'app_id'),)
    migrate(migrator.drop_column('formdef', 'app_name'),)


if __name__ == '__main__':
    main()