# can be used to set local test values for the variables that will be passed as command line arguments
# when run through the data platform.
# don't commit this file unless you change more than just the variables.

# set local values for command line args
report_module_script <- "mobile_user_monthly_lifetime_tables.R"
r_script_path <-'/home/mel/workspace/dimagi-data-platform/src/R'
config_path <-'/home/mel/workspace/dimagi-data-platform/src'
domain_list <-"'melissa-test-project','crs-imci'"# domains, comma-separated and surrounded by single quotes

# get the config parameters
source (file.path(r_script_path,"data_platform_funcs.R", fsep = .Platform$file.sep))
conf <- get_config(config_path)

# should the script use a csv file for testing instead of a database query?
use_csv <- TRUE

# run the report
source (file.path(r_script_path,report_module_script, fsep = .Platform$file.sep))