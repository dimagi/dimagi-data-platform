# called to run an R report module through Rscript, with command line arguments.

# get command line args (passed to Rscript)
args <- commandArgs(trailingOnly = TRUE)

report_module_script <- args[1] # the report module we're running
r_script_path <- args[2] # where to find the other scripts we're sourcing
config_path <- args[3] # where to find config.json
domain_list <- args[4] # list of domains to run for

# get the config parameters
source (file.path(r_script_path,"data_platform_funcs.R", fsep = .Platform$file.sep))
conf <- get_config(config_path)

# run the report
source (file.path(r_script_path,report_module_script, fsep = .Platform$file.sep))