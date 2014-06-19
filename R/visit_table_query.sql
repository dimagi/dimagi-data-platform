select * from visit,users
where visit.user_id = users.id
and domain like 'melissa-test-project'

--interactions
with a as (select visit_id, count (distinct form_id) as total_forms from form_visit group by visit_id)
SELECT visit.visit_id, users.user_id, cases.case_id, time_start, time_end, form_duration, a.total_forms,
time_start - lag(time_end, 1) OVER (PARTITION BY visit.user_id ORDER BY time_start) AS time_since_previous, users.domain
FROM visit, users, case_visit, cases, a
WHERE visit.user_id = users.id
and case_visit.case_id = cases.case_id
and case_visit.visit_id = visit.visit_id
and a.visit_id = visit.visit_id
order by visit.user_id, time_start

--form visits
SELECT visit.visit_id, users.user_id, form.form_id, form.time_start as form_start, form.time_end as form_end, visit.time_start, visit.time_end, form_duration
FROM visit, users, form_visit, form
WHERE visit.user_id = users.id
and form_visit.form_id = form.form_id
and form_visit.visit_id = visit.visit_id
and users.domain like 'melissa-test-project'
order by visit.user_id, visit.time_start