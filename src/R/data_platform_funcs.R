get_config <- function (config_path) {
  config_file = file.path(config_path,"config.json", fsep = .Platform$file.sep)
  library("jsonlite")
  conf<-fromJSON(config_file)$data_platform
  return(conf)
}