select * from aggregate_monthly_interactions where domain like 'melissa-test-project';
select count(id) from visit
select * from form where user_id in (select id from users where domain_id in (select id from domain where name like 'melissa-test-project'))

select visit.id,visit.user_id, visit.time_start, visit.time_end, count(form.id) from visit, form
where visit.user_id in (select id from users where domain_id in (select id from domain where name like 'melissa-test-project')) 
and form.visit_id = visit.id
group by visit.id,visit.user_id, visit.time_start, visit.time_end
order by visit.user_id, visit.time_start

select name from domain where id in (select domain_id from users) and active <> true order by name asc



select id, name from domain where name like 'tulasalud';

select * from users where user_id like '2789d51a5e4b44fc86f67e63d021946b'

select * from form where user_id = (select id from users where user_id like '2789d51a5e4b44fc86f67e63d021946b') order by time_start asc

select visit.id, count(form.id) as num_forms, visit.time_start, visit.time_end, users.user_id, domain.name as domain
            from form, visit, users, domain
            where domain.name like 'melissa-test-project'
            and form.visit_id = visit.id and visit.user_id = users.id and form.domain_id = domain.id
            group by visit.id, visit.time_start, visit.time_end, users.user_id, domain.name
            order by visit.time_start asc


select form.visit_id, form.id, form.form_id, form.time_start, form.time_end, cases.case_id, cases.parent_id from form,  case_event, cases
where form.user_id in (select id from users where domain_id = (select id from domain where name like 'tulasalud'))
and case_event.form_id = form.id and cases.id = case_event.case_id order by form.time_start asc

select form.visit_id, form.id, form.form_id, form.time_start, form.time_end, cases.case_id, cases.parent_id from form,  case_event, cases
where form.user_id in (select id from users where domain_id in (select id from domain where name like 'melissa-test-project')) 
and case_event.form_id = form.id and cases.id = case_event.case_id order by form.time_start asc

select * from cases where case_id like '4878fcaa-f77b-4686-8db8-a00c98355a55'

delete from visit where user_id in (select id from users where domain_id = (select id from domain where name like 'tulasalud'))
delete from visit where user_id in (select id from users where domain_id = (select id from domain where name like 'melissa-test-project'))

select * from cases where case_id like '8d4f8175-a9a1-4978-8ce8-c51682019e16'
select * from cases where parent_id like '8d4f8175-a9a1-4978-8ce8-c51682019e16'

