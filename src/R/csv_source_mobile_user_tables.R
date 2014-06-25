#specify these here since args will be null unless running from command line
domain_list <- "'melissa-test-project','crc-imci'"
r_script_path <- "/home/mel/workspace/dimagi-data-platform/src/R"
output_path <-"/home/mel/workspace/dimagi-data-platform/output"

setwd(output_path)

v <- read.csv("interaction_table_sample2.csv", header = TRUE)

# update timedate format (importing data from csv automatically makes it into factor variable in R)
v$time_start <- as.POSIXct(as.character(levels(v$time_start)), format = "%Y-%m-%d %H:%M:%S")[v$time_start]
v$time_end <- as.POSIXct(as.character(levels(v$time_end)), format = "%Y-%m-%d %H:%M:%S")[v$time_end]
v$visit_date <- as.Date(v$time_start)
v$month.index <- as.yearmon(v$visit_date) # obtaining year and month from Date
v$time_since_previous <- as.numeric(v$time_since_previous) # convert factor to numeric vectors
related_cases_index_child <- which(duplicated(v$time_start)) # this returns all child cases
v$related <- c("No")
v$related[related_cases_index_child] <- "Yes" # this flags all multiple-case visits