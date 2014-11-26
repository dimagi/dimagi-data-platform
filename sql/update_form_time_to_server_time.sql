update form set time_end = received_on, time_start = received_on - (time_end-time_start)
where @ EXTRACT(epoch FROM received_on - time_end)/(3600*24) > 30

select time_end, time_start, received_on,  received_on - (time_end-time_start)
from form
where @ EXTRACT(epoch FROM received_on - time_end)/(3600*24) > 30

select * from form
where @ EXTRACT(epoch FROM received_on - time_end)/(3600*24) > 30
limit 10

select * from form where id = 3245750

--3245750

