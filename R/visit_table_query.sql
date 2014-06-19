select * from visit,users
where visit.user_id = users.id
and domain like 'crs-imci'

SELECT users.user_id, cases.case_id, time_start, time_end, form_duration,  
time_start - lag(time_end, 1) OVER (PARTITION BY visit.user_id ORDER BY time_start) AS time_since_previous
FROM visit, users, case_visit, cases
WHERE visit.user_id = users.id
and case_visit.case_id = cases.case_id
and case_visit.visit_id = visit.visit_id
AND users.domain LIKE 'crs-imci'
order by visit.user_id, time_start