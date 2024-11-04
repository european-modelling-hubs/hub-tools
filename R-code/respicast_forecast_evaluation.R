library(scoringutils)
library(hubData)
library("dplyr")
library("optparse")
library(data.table)

# arguments 
# option_list = list(
#   make_option("--hub_path", type = "character", default = "./", help = "Hub path", metavar = "character"),
#   make_option("--truth_file_name", type = "character", default = "latest-ILI_incidence.csv", help = "Latest truth file name", metavar = "character"), 
#   make_option("--subfolders", type = "character", default = "ERVISS,FluID", help = "List of truth data folders", metavar = "character")
# );   

option_list = list(
  make_option("--hub_path", type = "character", default = "./", help = "Hub path", metavar = "character"),
  make_option("--targets", type = "character", default = "ILI_incidence,ARI_incidence", help = "Target names", metavar = "character"), 
  make_option("--subfolders", type = "character", default = "ERVISS,FluID", help = "List of truth data folders", metavar = "character")
);   

# Parse input
opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser)

# get targets and subfolders
targets_list <- strsplit(opt$targets, ",")
targets <- unlist(targets_list)

subfolders_list <- strsplit(opt$subfolders, ",")
subfolders <- unlist(subfolders_list)

overall_scores <- data.table()

# for each target, extract data
for (target in targets) {

  # Read the truth data
  # and add all to the truth_data dataframe
  truth_file_name <- paste0("latest-", target, ".csv")
  
  truth_data <- data.frame()
  for (subfolder in subfolders) {
      truth_data_temp <- read.csv(paste0(opt$hub_path, "/target-data/", subfolder, "/", truth_file_name), header = TRUE)
      truth_data <- rbind(truth_data, truth_data_temp)
  }

  # rename truth_date column to target_end_date and value column to true_value
  truth_data <- truth_data %>% 
        rename("target_end_date" = "truth_date",
               "true_value" = "value")

  # drop the year_week column from df
  truth_data <- truth_data[, !names(truth_data) %in% "year_week"]

  # and convert target_end_date to Date format,  "%Y-%m-%d"
  truth_data$target_end_date <- as.Date(truth_data$target_end_date, format = "%Y-%m-%d")

  # connect to hub
  model_outputs <- hubData::connect_hub(hub_path = opt$hub_path) %>%
    dplyr::collect()

  # drop rows not relating to current target
  curr_target <- gsub("_", " ", target)
  model_outputs <- model_outputs[model_outputs$target %in% curr_target,]
  
  # rename model output columns 
  model_outputs <- model_outputs %>% 
          rename("quantile" = "output_type_id",
                "prediction" = "value",
                "model" = "model_id")

  # join model output and truth (left join)
  full_data <- merge(model_outputs, truth_data, by = c("target", "target_end_date", "location"), all.x = TRUE)

  # remove rows related to median and where true data is null
  full_data <- full_data[full_data$output_type != "median", ]
  full_data <- full_data[complete.cases(full_data$true_value), ]

  # compute scoring metrics
  forecast_scores <- set_forecast_unit(
                          full_data,
                          c("origin_date", "target", "target_end_date", "horizon", "location", "model")
                      ) %>%
                      check_forecasts() %>%
                      score(metrics=c("ae_median", "interval_score"))

  # summarize scores
  summ_scores <- forecast_scores %>%
    summarise_scores()  

  overall_scores <- rbind(overall_scores, summ_scores)
}




# save
write.csv(overall_scores, file = paste0(opt$hub_path, "/model-evaluation/forecast_scores_summary.csv"), row.names = FALSE)
