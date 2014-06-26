library(RPostgreSQL)

drv <- dbDriver("PostgreSQL")

# should read DB conn properties from config file
con <- dbConnect(drv, dbname=conf$database$dbname,
                 user=conf$database$user,
                 pass=conf$database$pass,
                 host=conf$database$host, 
                 port=conf$database$port)

domain_list <- paste(lapply(conf$domains$name,sprintf,fmt="'%s'"),sep=" ", collapse=",")
query <- sprintf("with a as 
                 (select visit_id, count (distinct form_id) as total_forms 
                 from form_visit 
                 group by visit_id), 
                 b as 
                 (select visit.visit_id,  date_part('epoch',time_start - lag(time_end, 1) 
                 over (partition by visit.user_id order by time_start)) as time_since_previous 
                 from visit 
                 order by visit.user_id, time_start) 
                 select visit.visit_id, users.user_id, cases.case_id, time_start, time_end, form_duration, 
                 a.total_forms, b.time_since_previous, visit.home_visit, users.domain 
                 from visit, users, case_visit, cases, a,b 
                 where visit.user_id = users.id 
                 and case_visit.case_id = cases.id 
                 and case_visit.visit_id = visit.visit_id 
                 and a.visit_id = visit.visit_id 
                 and b.visit_id = visit.visit_id 
                 and users.domain in (%s) 
                 order by visit.user_id, time_start", domain_list)

rs <- dbSendQuery(con,query)
v <- fetch(rs,n=-1)