# install/import libraries 
library(zoo)
library(digest)
library(plyr)
library(timeDate)
library(reshape2)

# get command line args: domain list and output dir
args <- commandArgs(trailingOnly = TRUE)

domain_list <- args[1]
r_script_path <- args[2]
output_path <- args[3]

#domain_list <- "'melissa-test-project','crc-imci'"
#r_script_path <- "/home/mel/workspace/dimagi-data-platform/src/R"
#output_path <-"/home/mel/workspace/dimagi-data-platform/output"

# get data source
data_source_file = file.path(r_script_path,"db_source_mobile_user_tables.R", fsep = .Platform$file.sep)
# data_source_file = file.path(r_script_path,"csv_source_mobile_user_tables.R", fsep = .Platform$file.sep)
source(data_source_file,chdir=T)

setwd(output_path)

# get visit date from time start NOTE may be arbitrary - phone timestamps not necessarily correct
v$visit_date <- as.Date(v$time_start)
v$month.index <- as.yearmon(v$visit_date) # obtaining year and month from Date
related_cases_index_child <- which(duplicated(v$time_start)) # this returns all child cases
v$related <- c("No")
v$related[related_cases_index_child] <- "Yes" # this flags all multiple-case visits

# remove demo_user
v0 <- v[which(v$user_id != "demo_user"),]
v0$user_id <- factor(v0$user_id)


# split into a list of data frames to operate
# split into a list of data frames to operate
library(plyr)
v1 <- dlply(v0, .(domain))

for (i in seq_along(v1)) {
  v1[[i]]$domain.index <- i 
} # assign domain index 

# operate on list of data frames
v2 <- lapply(v1, function(x){
  nvisits_raw <- as.data.frame(table(x$user_id)) 
  nvisits_raw <- nvisits_raw[order(nvisits_raw$Var1), ]
  user_remove <- nvisits_raw$Var1[which(nvisits_raw$Freq == 1)] # this returns user_id with only one visit in their whole CC lifecycle
  x <- x[!x$user_id %in% user_remove,] # this removes users who have been active only for once in their CC lifecycle
  x$user_id <- factor(x$user_id)
  x <- x[order(x$user_id, x$time_start), ]
  x$date_difference <- c(NA, diff(x$visit_date)) # this examines if sorted visits happen in the same or different days
  x$time_since_previous[which(x$time_since_previous == "")] <- NA # this seems to be "NA" already if you import that in R in linux (not sure why it's missing here on Mac)
  x$visit_weekday <- as.numeric(format(x$visit_date, "%w"))
  x$time_since_previous <- as.numeric(x$time_since_previous)
  x$batch_entry <- ifelse(x$date_difference == 0 & x$time_since_previous < 600, 1, 0)  # time_since_previous is calculated in seconds
  x$time_ffs <- strftime(x$time_start, format = "%H:%M:%S")
  x$visit_hour <- strftime(x$time_start, format = "%H") # extract hour from time vector
  x$visit_time <- ifelse(x$time_ffs >= "06:00:00" & x$time_ffs < "12:00:00", "morning", 
                         ifelse(x$time_ffs >= "12:00:00" & x$time_ffs < "18:00:00", "afternoon",
                                ifelse(x$time_ffs >= "18:00:00" & x$time_ffs < "24:00:00", "night", "after midnight")))
  x <- x[order(x$case_id, x$time_start), ]
  x$days_elapsed_case <- c(NA, as.numeric(diff(x$visit_date, units = "days")))
  x$days_elapsed_case[which(diff(as.numeric(factor(x$case_id, ordered = TRUE))) != 0) + 1] <- NA # flagged child_case visits are excluded from calculation
  x$days_elapsed_case[1] <- NA
  x$new_case <- ifelse(is.na(x$days_elapsed_case) == TRUE, 1, 0) # this flags all visits registering a new case
  x$follow_up <- ifelse(is.na(x$days_elapsed_case) == FALSE, 1, 0) # likewise, flagging all followup visits
  return(x)
})
# this computes average number of cases visited in an hour for each domain, will be added to user-indexed lifetime table later
v3 <- lapply(v2, function(x){
  y <- as.data.frame(table(x$user_id, x$visit_hour)) 
  names(y) <- c("user_id", "visit_hour", "cases_visited_per_hour")
  y <- y[order(y$user_id, y$visit_hour),]
  y1 <- y[which(y$cases_visited_per_hour != 0),]  # this returns active visit_hour only. Those non-active visit_hour index are excluded from calculating mean
  y1_aggr <- aggregate(y1$cases_visited_per_hour, list(y1$user_id), FUN = mean) # this returns the average number of cases that had interactions with user in an active hour
  names(y1_aggr) <- c("user_id", "avg.cases_visited_per_active_hour")
  y1_aggr$avg.cases_visited_per_active_hour <- round(y1_aggr$avg.cases_visited_per_active_hour, 2)
  return(y1_aggr) # this would be an indicator for flw lifetime and monthly table
})
# compute user indexed table
f0 <- lapply(v2, function(x) {
  x$user_id <- factor(x$user_id)
  x <- x[order(x$user_id, x$time_start), ] # sort visits by time_start for a given user
  temp1 <- duplicated(x$user_id) # this goes through all visits by the same mobile user and return logical results, first visit = FALSE, all following visits are duplicates & TRUE
  temp2 <- duplicated(x$user_id, fromLast = TRUE)
  first.visit <- x[temp1 == FALSE, ] # this returns the first visit done each unqiue mobile user
  last.visit <- x[temp2 == FALSE, ] # this returns the last visit by each unique mobile user
  nvisits <- as.data.frame(table(x$user_id)) # this returns total visits of a user in his CommCare lifecycle
  nvisits <- nvisits[order(nvisits$Var1), ] # sort data by user_id
  first.s <- first.visit[order(first.visit$user_id), ]
  last.s <- last.visit[order(last.visit$user_id), ]
  days_on_cc <- as.numeric(difftime(last.s$visit_date, first.s$visit_date, units = "days")) + 1  # this returns total days between the first and the last form submission by a mobile user
  days_visit_last <- as.numeric(difftime(Sys.Date(), last.s$visit_date, units = "days")) # this returns days elapsed between two visits by a user
  y <- data.frame(first.s$user_id, first.s$visit_date, last.s$visit_date, nvisits$Freq, days_on_cc, days_visit_last)
  names(y) <- c("user_id", "date_first_visit", "date_last_visit", "nvisits", "days_on_cc", "days_visit_last")
  y$date_first_visit <- as.Date(strptime(y$date_first_visit, format = "%Y-%m-%d"))
  y$date_last_visit <- as.Date(strptime(y$date_last_visit, format = "%Y-%m-%d"))
  y$active_user <- ifelse(y$days_visit_last <= 30, 1, 0) # active user is defined as having form submission within last 30 days
  
  y$calendar_month_start <- as.Date(as.yearmon(y$date_first_visit) + 1/12, frac = 0) # incomplete calendar month would not be 
  y$calendar_month_end <- as.Date(as.yearmon(y$date_last_visit), frac = 0) 
  next.month <- function(d) as.Date(as.yearmon(d) + 1/12) + 
    as.numeric(d - as.Date(as.yearmon(d)))
  next1 <- next.month(y$date_first_visit) # this returns the same Date in the following month
  index1 <- which(next1 == y$calendar_month_start)    # this returns the index of mobile users with a starting time on the first day in a month 
  if(length(index1)) y$calendar_month_start[index1] <- as.Date(as.yearmon(y$calendar_month_start[index1]) - 1/12, frac = 0) 
  
  next2 <- next.month(y$calendar_month_end)
  index2 <- which(as.numeric(difftime(next2, y$date_last_visit)) == 1) # this returns the index of mobile users who have a last_visit_date on the last day in a month
  if(length(index2))  y$calendar_month_end[index2] <- as.Date(as.yearmon(y$calendar_month_end[index2]) + 1/12, frac = 0) # update calendar month end for mobile users with a last visit date on the day ending the month
  y$calendar_months_on_cc <- 12*(as.yearmon(y$calendar_month_end) - as.yearmon(y$calendar_month_start)) # later to remove all users with total calendar months on CC < 3
  y$calendar_months_on_cc <- ifelse(y$calendar_months_on_cc < 0, 0, y$calendar_months_on_cc)
  
  activity <- as.data.frame(table(x$user_id, x$month.index)) # this returns number of total visits per month by a given user
  activity.count <- aggregate(activity$Var2, list(activity$Var1), function(x) length(unique(x))) # this returns number of unique months a worker is actively visiting cases
  activity.count.s <- activity.count[order(activity.count$Group.1), ]
  y$active_months <- activity.count.s$x - 2 # dropping the first and last active month of a given mobile worker
  if(length(index1)) y$active_months[index1] <- y$active_months[index1] + 1 # Adding one month back for users with a first visit on month begin
  if(length(index2)) y$active_months[index2] <- y$active_months[index2] + 1 # FAdding one month back for users with a last visit on month end
  y$active_months <- ifelse(y$calendar_months_on_cc == 0, 0, y$active_months)
  y$active_month_percent <- as.numeric(round(y$active_months/y$calendar_months_on_cc, 2))  # denominator can be 0, active_month_percent in this case is NaN
  activity_by_day <- as.data.frame(table(x$user_id, x$visit_date))
  activity_by_day <- activity_by_day[which(activity_by_day$Freq != 0),] # this returns visits on each active day by a user
  activity_by_day.count <- aggregate(activity_by_day$Var2, list(activity_by_day$Var1), function(x) length(unique(x))) # total active days of a user
  activity_by_day.count.s <- activity_by_day.count[order(activity_by_day.count$Group.1), ]
  y$active_days <- activity_by_day.count.s$x
  y$active_day_percent <- round(y$active_days/y$days_on_cc, 2) # denominators would never be 0 in this calculation
  nforms <- aggregate(total_forms~user_id, data = x, sum) # this returns total forms submitted in all visits by a given user
  nforms <- nforms[order(nforms$user_id), ]
  y$nforms <- nforms$forms_per_visit
  visit_duration <- aggregate(form_duration~user_id, data = x, median) # MEDIAN TIME DURATION PER VISIT IN SECs
  y$median_visit_duration <- as.numeric(round(visit_duration$form_duration/60, 2)) # in minutes
  d <- as.data.frame(table(x$visit_date, x$user_id)) # this returns number of unique visits in a day by a given mobile user
  d <- subset(d, Freq != 0)
  dsub <- d[order(d$Var2), ] 
  d.median <- aggregate(dsub$Freq, list(dsub$Var2), median, na.rm = TRUE) 
  y$median_visits_per_day <- d.median$x  # this returns the median of unique visits in a day by a given user
  m <- as.data.frame(table(x$month.index, x$user_id))
  m <- subset(m, Freq != 0)
  msub <- m[order(m$Var2), ]
  m.median <- aggregate(msub$Freq, list(msub$Var2), median, na.rm = TRUE)
  y$median_visits_per_month <- m.median$x
  
  v <- x[order(x$user_id, x$time_start), ] # sort visits by time_start
  median_time_elapse <- with(v, aggregate(time_since_previous, list(user_id), FUN = median, na.rm = TRUE))
  mean_time_elapse <- with(v, aggregate(time_since_previous, list(user_id), FUN = mean, na.rm = TRUE))
  y$median_time_btw_visits <- median_time_elapse$x
  y$mean_time_elapse_btw_visits <- mean_time_elapse$x
  c <- x[order(x$user_id, x$case_id, x$time_start), ]
  ct1 <- tail(c$time_start, -1)
  ct2 <- head(c$time_end, -1)
  c$mins_elapse_flw_same_case <- c(NA, as.numeric(difftime(ct1, ct2, units = "mins")))
  c$mins_elapse_flw_same_case[which(diff(as.numeric(factor(c$user_id, ordered = TRUE))) != 0) + 1] <- NA # EACH FLW'S FIRST VISIT SHOULD BE REPLACED WITH NA
  c$mins_elapse_flw_same_case[which(diff(as.numeric(factor(c$case_id, ordered = TRUE))) != 0) + 1] <- NA # EACH FLW'S FIRST VISIT TO A DIFFERENT CASE SHOULD ALSO BE REPLACED WITH NA
  c1 <- with(c, aggregate(mins_elapse_flw_same_case, list(user_id, case_id), FUN = median, na.rm = TRUE)) # NA STILL EXISTS FOR CASES WITH 1 VISIT ONLY
  c2 <- aggregate(c1$x, list(c1$Group.1), median, na.rm = TRUE)
  c3 <- aggregate(c1$x, list(c1$Group.1), mean, na.rm = TRUE)
  c2 <- c2[order(c2$Group.1), ]
  c3 <- c3[order(c3$Group.1), ]
  y$median_time_btw_followup <- c2$x # IN MINUTES
  y$median_days_btw_followup <- round(y$median_time_btw_followup/60/24, 2)
  y$mean_time_btw_followup <- c3$x # IN MINUTES
  y$mean_days_btw_followup <- round(y$mean_time_btw_followup/60/24, 2) # convert minutes into days
  temp <- as.matrix(table(x$user_id, x$batch_entry)) # this returns a matrix of batch/non-batch for each unique worker
  if(length(dimnames(temp)) > 2) { # meaning there are both batch and non-batch visits in a domain
    batch <- as.data.frame(temp[, "1"]) 
    user_id <- dimnames(batch)[[1]]
    be <- cbind(batch, user_id)  
    y <- merge(be, y, by = "user_id", all.y = TRUE) # this merges batch_visit data frame with the big data frame, extra rows in be will be excluded
    y$batch_entry_percent <- round(y$tot_batch_visits/y$nvisits, 2)
  }else if(dimnames(temp)[2] %in% "1"){
    y$tot_batch_visits <- y$nvisits
    y$batch_entry_percent <- 1}else{
      y$tot_batch_visits <- 0
      y$batch_entry_percent <- 0}        
  y$domain.index <- rep(x$domain.index[1], nrow(y)) # adding domain index and domain name to lifetime table
  y$domain.name <- rep(x$domain[1], nrow(y))
  y$domain.index <- factor(y$domain.index)
  
  # compute total number of cases registered by a user
  new_case <- ddply(x, .(user_id), function(x) length(which(x$new_case == 1))) # this returns total new cases with a first visit 
  follow_up <- ddply(x, .(user_id), function(x) length(which(x$follow_up == 1)))
  colnames(new_case)[2] <- c("new_cases") # rename variables would make it cleaner when we merge all data frames
  colnames(follow_up)[2] <- c("follow_up")
  
  merge_me <- list(y, new_case, follow_up)
  y1 <- Reduce(function(...) merge(..., all=T), merge_me)
  return(y1)
})
visit_time_percentage <- lapply(v2, function(x){
  ddply(x, .(user_id), function(x){
    r1 <- data.frame(table(x$visit_time)/nrow(x))
    names(r1) <- c('time', 'proportion')
    r1$proportion <- round(100*r1$proportion, 2)
    return(r1)    
  })
})
reshaped_visit_time <- lapply(visit_time_percentage, function(x){
  r2 <- dcast(x, user_id~time)
  return(r2)
})
# adding avg.cases visited per hour to user-indexed table
# apply merge to two lists of data frames
f1 <- do.call("rbind", f0) # combine multiple 
avg.cases <- do.call("rbind", v3) 
reshaped_visit_time <- do.call("rbind.fill", reshaped_visit_time)
merge_me <- list(f1, avg.cases, reshaped_visit_time)
f2 <- Reduce(function(...) merge(..., all=T), merge_me)
f2$case_followup_rate <- round(f2$follow_up/f2$new_cases, 2)
######################################
# Monthly table computation
user_list <- lapply(v2, function(x) {
  x.split <- split(x, x$user_id)
  return(x.split)
}) # this splits visit tables of each domain into tables for each flw

# right now user_list is a list of list. should be converted to a single list for monthly table computation
user_list <- unlist(user_list, recursive = F) # unlist does the trick here
user_list_1 <- lapply(user_list, function(x) {
  arrange(x, time_start) # sort visit table by first form submission fir each user  
  x$bar1 <- as.Date(timeFirstDayInMonth(as.character(x$visit_date), format = "%Y-%m-%d")) # this actually repeats the work done in lifetime table but for now i will just leave it the way it is
  x$bar2 <- as.Date(timeLastDayInMonth(as.character(x$visit_date), format = "%Y-%m-%d"))
  # x$month.index <- as.factor(format(x$month.index, "%Y-%m")) 
  # x$month.index <- factor(x$month.index, levels(x$month.index)) # these two lines refactoring yearmon() format var month.index can be optimized earlier when computing visit table
  return(x)
})
m <- lapply(user_list_1, function(x){
  y1 <- aggregate(follow_up~month.index, data = x, sum) # FOLLOW_UP VISITS PER MONTH (NOTE: MULTIPLE FOLLOWUPS TO ONE GIVEN CASE IS POSSIBLE THUS FOLLOW_UP VISITS != UNIQUE CASES BEING FOLLOWED UP)
  names(y1) <- c("month.index", "follow_up_visits")
  y1$month.index <- as.factor(y1$month.index)
  y2 <- ddply(x, .(month.index), function(x) length(unique(x[x$follow_up == 1, ]$case_id)))
  names(y2) <- c("month.index", "follow_up_unique_case") # This calculates follow up visits to a unique case in an active month 
  y2$month.index <- as.factor(y2$month.index)
  t1 <- as.data.frame(table(x$new_case, x$month.index))
  names(t1) <- c("new_case", "month.index", "count")
  t2 <- ddply(t1, .(month.index), function(x) sum(x[x$new_case == 1, ]$count)) # This calcualates new cases opened in each active month
  names(t2) <- c("month.index", "case_registered")
  t2$cum_case_registered <- cumsum(t2$case_registered) # could be an empty data frame if there is no new case in a month
  # this calculates proportion of visits in different time period in a day (am,pm,after-pm, etc)
  res1 <- ddply(x, c("user_id", "month.index"), function(x){
    r1 <- data.frame(table(x$visit_time)/nrow(x))
    names(r1) <- c('time', 'proportion')
    r1$proportion <- round(100*r1$proportion, 2)
    return(r1)    
  })
  res1_cast <- dcast(res1, user_id + month.index ~ time) # using...override will show up. That's not an err. Don't panic
  res1_cast$month.index <- as.factor(res1_cast$month.index)
  
  if(length(t2)) merge_me <- list(y1,y2,t2, res1_cast) else merge_me <- list(y1,y2, res1_cast)
  y3 <- Reduce(function(...) merge(..., all=T), merge_me)
  y3$unique_case_followup_rate <- round(100*y3$follow_up_unique_case/y3$cum_case_registered, 2) # MIGHT BE INF WHEN THE FLW ONLY VISITS SHARED CASES NOT OPENED BY HER
  y3$user_id <- x$user_id[1] # PASTE FLWID TO MONTHLY TABLE 
  y3$domain.index <- rep(x$domain.index[1], nrow(y3))
  y3 <- arrange(y3, y3$month.index)
  
  return(y3)
})
m1 <- lapply(user_list_1, function(x){
  y1 <- as.data.frame(table(x$month.index))
  names(y1) <- c("month.index", "visits") # this calculates total visits in each active month
  y2 <- aggregate(x$total_forms~x$month.index, data = x, sum)
  names(y2) <- c("month.index", "nforms_per_month") # total form submissions in each active month
  y2$month.index <- as.factor(y2$month.index)
  y3 <- aggregate(x$visit_date, list(x$month.index), function(x) length(unique(x)))
  names(y3) <- c("month.index", "active_days_per_month") # days with at least one visit in each active month
  y3$month.index <- as.factor(y3$month.index)
  y3$active_days_percent <- round(100*y3$active_days_per_month/30, 2) # demoninator should be specified number of days in each month...
  y4 <- aggregate(visit_date~month.index, data = x, min)
  names(y4) <- c("month.index", "first_visit_date") # FIRST_ACTIVE_DAY IN A GIVEN MONTH
  y4$month.index <- as.factor(y4$month.index)
  y5 <- aggregate(visit_date~month.index, data = x, max)
  names(y5) <- c("month.index", "last_visit_date") # LAST_ACTIVE_DAY IN A GIVEN MONTH
  y5$month.index <- as.factor(y5$month.index)
  y5$days_on_cc <- as.numeric(y5$last_visit_date - y4$first_visit_date) + 1 # DAYS ON CC (FIRST_ACTIVE_DAY ~ LAST_ACTIVE_DAY) IN A GIVEN MONTH
  
  y6 <- aggregate(batch_entry~month.index, data = x, sum, na.action = na.pass)
  y6$batch_entry_percent <- round(100*y6$batch_entry/y1$visits, 2) # BATCH ENTRY VISITS/TOTAL VISITS PER MONTH
  y6$month.index <- as.factor(y6$month.index)
  tot_new_case <- length(which(x$new_case == 1)) # TOTAL REGISTERED CASES
  y7 <- aggregate(new_case~month.index, data = x, sum)
  y7$month.index <- as.factor(y7$month.index)
  y7$new_case_percent <- round(100*y7$new_case/tot_new_case, 2) # OPENED CASE/REGISTERED CASE
  y8 <- aggregate(follow_up~month.index, data = x, sum) # FOLLOW_UP VISITS PER MONTH (NOTE: MULTIPLE FOLLOWUPS TO ONE GIVEN CASE IS POSSIBLE THUS FOLLOW_UP VISITS != UNIQUE CASES BEING FOLLOWED UP)
  names(y8) <- c("month.index", "tot_follow_up")
  y8$month.index <- as.factor(y8$month.index)
  y8$follow_up_percent <- round(100*y8$tot_follow_up/y1$visits, 2)
  y9 <- ddply(x, .(month.index), function(x) length(unique(x[x$follow_up == 1, ]$caseid)))
  names(y9) <- c("month.index", "follow_up_unique_case")
  y9$month.index <- as.factor(y9$month.index) # can write a ddply function instead of changing format one by one but for now just leave it here
  
  y_temp <- as.data.frame(table(x$visit_date))
  names(y_temp) <- c("active_date", "nvisits")
  y_temp$month.index <- as.factor(as.yearmon(y_temp$active_date))
  y10 <- aggregate(nvisits~month.index, data = y_temp, median)
  names(y10) <- c("month.index", "median_visits_per_active_day")
  y10$month.index <- as.factor(y10$month.index)
  y11 <- aggregate(form_duration~month.index, data = x, median)
  names(y11) <- c("month.index", "median_visit_duration")
  y11$month.index <- as.factor(y11$month.index)
  y11$median_visit_duration <- round(y11$median_visit_duration/60, 2)
  
  
  merge_me <- list(y1, y2, y3, y4, y5, y6, y7, y8, y9, y10, y11) 
  y12 <- Reduce(function(...) merge(..., all=T), merge_me)
  y12$domain.index <- rep(x$domain.index[1], nrow(y12))
  y12$user_id <- rep(x$user_id[1], nrow(y12))
  y12 <- arrange(y12, y12$month.index)
  y12$new_case_or_not <- NULL
  
  return(y12)
})
# join data frames in M and M1. This returns the monthly table for each user throughout their lifetime on CommCare
monthly_table <- Reduce(function(x,y) Map(cbind, x, y),list(m, m1)) # this does not filter out any flw with only a few visits in one incomplete calendar month
monthly <- lapply(monthly_table, function(x){x[!duplicated(lapply(x, digest))]})
monthly_merge <- do.call("rbind.fill", monthly)
#################################
mainDir <- getwd()
subDir0 <- "monthly"
dir.create(file.path(mainDir, subDir0))
setwd(file.path(mainDir, subDir0))
write.csv(monthly_merge, "monthly_merge.csv") # this keeps the monthly table output into wherever the working directory of R session is
#export visit table output
subDir1 <- "visit"
dir.create(file.path(mainDir, subDir1))
setwd(file.path(mainDir, subDir1))
visitOut <- function(x) {
  for (i in seq_along(x)) {
    filename[i] <- paste(x[[i]]$domain[1], sep = "", ".csv")
    write.csv(x[[i]], filename[i])
  }
}
visitOut(v2)
# export lifetime table 
subDir2 <- "lifetime"
dir.create(file.path(mainDir, subDir2))
setwd(file.path(mainDir, subDir2))
lifetime.split <- split(f2, f2$domain.name)

lifeOut <- function(x) {
  for (i in seq_along(x)) {
    filename[i] = paste(x[[i]]$domain.name[1], ".csv")
    write.csv(x[[i]], filename[i])
  }
}
lifeOut(lifetime.split) 
######################################
# Rashmi: attrition report
