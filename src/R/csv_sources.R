get_interaction_table_from_csv <- function (filename) {

v <- read.csv(filename, header = TRUE, stringsAsFactors = FALSE)

return (v)
}