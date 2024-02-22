library(hubUtils)
library(dplyr)
library(hubEnsembles)
library(jsonlite)
library(yaml)
library(purrr)
library("optparse")
# arguments 
option_list = list(
  make_option("--hub_path", type = "character", default = "./", help = "Hub path", metavar = "character"),
  make_option("--agg_fun", type = "character", default = "median", help = "Aggregating function to use for ensemble [default= %default]", metavar = "character"), 
  make_option("--model_id", type = "character", default = "hubEnsemble", help = "Ensemble model abbreviation [default= %default]", metavar = "character"),
  make_option("--team_id", type = "character", default = "respicast", help = "Ensemble team abbreviation [default= %default]", metavar = "character")
);   
 
opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser)
# connect to hub
model_outputs <- hubUtils::connect_hub(hub_path = opt$hub_path) %>%
  dplyr::collect()
# get list of models to include in the ensemble
yaml_files <- list.files(path = file.path(opt$hub_path, "model-metadata"), pattern = "\\.yml$", full.names = TRUE)
models <- map(yaml_files, ~{
  content <- yaml::yaml.load_file(.x)
  if (content$team_model_designation %in% c("primary", "secondary")) {
    return(paste(content$team_abbr, content$model_abbr, sep = "-"))
  }
  return(NULL)
}) %>%
  compact()  # Remove NULL entries

# select only rows related to these models
model_outputs <- model_outputs[model_outputs$model_id %in% models, ]

# exclude NA rows
model_outputs <- subset(model_outputs, !is.na(origin_date))

# select only rows related to most recent forecast
max_origin_date = max(model_outputs$origin_date)
model_outputs <- model_outputs[model_outputs$origin_date == max_origin_date, ]

# exclude models with extreme values
model_outputs <- model_outputs %>%
  group_by(model_id, location) %>%
  filter(all(value <= 30000))

# generate ensemble
ens <- simple_ensemble(model_outputs,
                       agg_fun = opt$agg_fun,
                       model_id = opt$model_id,
                       task_id_cols = c("origin_date", "target", "target_end_date", "horizon", "location"))
# remove model_id column
ens <- ens %>% select(-model_id)
# change NA to empty string
ens[] <- lapply(ens, function(x) ifelse(is.na(x), "", x))
# cast date columns
ens$target_end_date <- as.Date(ens$target_end_date)
ens$origin_date <- as.Date(ens$origin_date)
# write
save_path = file.path(opt$hub_path, "model-output", paste(opt$team_id, opt$model_id, sep = "-"))
file_name <- paste(as.Date(max_origin_date), opt$team_id, opt$model_id, sep = "-")
full_path <- file.path(save_path, paste0(file_name, ".csv"))
write.csv(ens, file = full_path, row.names = FALSE)
# create json with ensemble models
max_origin_date_fmt = format(as.Date(max_origin_date))
ensemble_models <- list()
unique_targets <- unique(model_outputs$target)
unique_locations <- unique(model_outputs$location)
unique_horizons <- unique(model_outputs$horizon)
ensemble_models <- lapply(unique_targets, function(target) {
  target_list <- list(target = target, countries = lapply(unique_locations, function(location) {
    country_list <- list(id = location, members = lapply(unique_horizons, function(horizon) {
      subset_data <- model_outputs[model_outputs$target == target & model_outputs$location == location & model_outputs$horizon == horizon, ]
      horizon_list <- list(horizon = horizon, models = unique(subset_data$model_id))
    }))
  }))
})
# Write to JSON
save_path_json = file.path(opt$hub_path, ".github/logs/ensemble-members/")
file_name_json <- paste(max_origin_date_fmt, opt$team_id, opt$model_id, "ensemble_models.json", sep = "-")
write_json(ensemble_models, path = paste0(save_path_json, file_name_json))


env_file <- Sys.getenv("GITHUB_OUTPUT")
save_path_env = file.path("model-output", paste(opt$team_id, opt$model_id, sep = "-"))
file_name_env <- paste(as.Date(max_origin_date), opt$team_id, opt$model_id, sep = "-")
full_path_env <- file.path(save_path_env, paste0(file_name_env, ".csv"))
ensemble_file <- paste0("ensemble_file=", full_path_env)

# Open the file in append mode and write the baseline_file content
cat(ensemble_file, file = env_file, append = TRUE)
