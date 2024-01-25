library(scoringutils)
library(hubUtils)
library("dplyr")
library("optparse")

# arguments 
option_list = list(
  make_option("--hub_path", type = "character", default = "./", help = "Hub path", metavar = "character"),
  make_option("--truth_file_name", type = "character", default = "latest-ILI_incidence.csv", help = "Latest truth file name", metavar = "character"), 
  make_option("--subfolders", type = "character", default = "ERVISS,FluID", help = "List of truth data folders", metavar = "character")
);   
 
opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser)

# Read the truth data
subfolders_list <- strsplit(opt$subfolders, ", ")
subfolders <- unlist(subfolders_list)

truth_data <- data.frame()
for (subfolder in subfolders) {
    truth_data_temp <- read.csv(paste0(opt$hub_path, "/target-data/", subfolder, "/", opt$truth_file_name), header = TRUE)
    truth_data <- rbind(truth_data, truth_data_temp)
}

# rename truth columns 
truth_data <- truth_data %>% 
        rename("target_end_date" = "truth_date",
               "true_value" = "value")
truth_data <- truth_data[, !names(truth_data) %in% "year_week"]
truth_data$target_end_date <- as.Date(truth_data$target_end_date, format = "%Y-%m-%d")


# connect to hub
model_outputs <- hubUtils::connect_hub(hub_path = opt$hub_path) %>%
  dplyr::collect()

# rename model output columns 
model_outputs <- model_outputs %>% 
        rename("quantile" = "output_type_id",
               "prediction" = "value",
               "model" = "model_id")

# join model output and truth (left join)
full_data <- merge(model_outputs, truth_data, by = c("target_end_date", "location"), all.x = TRUE)

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

# save
write.csv(summ_scores, file = paste0(opt$hub_path, "/model-evaluation/forecast_scores_summary.csv"), row.names = FALSE)
