import pandas as pd 
import numpy as np
import argparse
import os

parser = argparse.ArgumentParser()
parser.add_argument('--hub_path', default="./")
parser.add_argument('--baseline_team_abbr', default="respicast")
parser.add_argument('--baseline_model_abbr', default="quantileBaseline")

args = parser.parse_args()
metric_names = {"interval_score": "WIS", "ae_median": "AE"}

# import forecast scoring 
df_scores = pd.read_csv(os.path.join(args.hub_path, f"model-evaluation/forecast_scores_summary.csv"))

# create team_id, model_id columns
df_scores["team_id"] = df_scores["model"].apply(lambda x : str(x).split("-")[0])
df_scores["model_id"] = df_scores["model"].apply(lambda x : str(x).split("-")[1])
df_scores.drop("model", inplace=True, axis=1)

# cols to rows
df_scores = pd.melt(df_scores, 
        id_vars = ['origin_date', 'target', 'target_end_date', 'horizon', 'location', "team_id", "model_id"],
        value_vars=['interval_score', 'dispersion', 'underprediction', 'overprediction', 'ae_median'], 
        var_name='metric', value_name='value_absolute')

# keep selected metrics and rename
df_scores = df_scores.loc[df_scores.metric.isin(["interval_score", "ae_median"])].reset_index(drop=True)
df_scores["metric"] = df_scores["metric"].apply(lambda x : metric_names[x])

# Filter baseline data
baseline_filter = (df_scores['team_id'] == str(args.baseline_team_abbr)) & \
                  (df_scores['model_id'] == str(args.baseline_model_abbr))
baseline_data = df_scores.loc[baseline_filter]

# Compute relative values
df_scores["value_relative"] = np.nan
for index, row in df_scores.iterrows():
   temp_baseline = baseline_data.loc[(baseline_data['origin_date'] == row['origin_date']) & \
                                      (baseline_data['target'] == row['target']) & \
                                      (baseline_data['target_end_date'] == row['target_end_date']) & \
                                      (baseline_data['horizon'] == row['horizon']) & \
                                      (baseline_data['location'] == row['location']) & \
                                      (baseline_data['metric'] == row['metric'])]
   if not temp_baseline.empty:
      #df_scores.at[index, 'value_relative'] = np.log2(temp_baseline['value_absolute'].values[0] / row['value_absolute'])
      df_scores.at[index, 'value_relative'] = 1 - row.value_absolute / temp_baseline.value_absolute.values[0]

# remove rows where value relative is NaN
df_scores = df_scores.loc[df_scores.value_relative.notnull()].reset_index(drop=True)

# compute number of models 
df_nmodels = df_scores.groupby(by=["origin_date", "target", "target_end_date", "horizon", "location", "metric"], as_index=False).model_id.nunique()
df_nmodels.rename(columns={"model_id": "n_models"}, inplace=True)
df_scores = pd.merge(left=df_scores, right=df_nmodels, on=["origin_date", "target", "target_end_date", "horizon", "location", "metric"], how="left")

# compute rank
df_scores["rank"] = df_scores.groupby(by=["origin_date", "target", "target_end_date", "horizon", "location", "metric"], as_index=False).value_absolute.rank(method="min")
df_scores["rank"] = df_scores["rank"].astype(int)

# compute rank_score
forecast_rounds = df_scores[['origin_date', 'target', 'target_end_date', 
                             'horizon', 'location', 'metric']].drop_duplicates()
df_scores["rank_score"] = np.nan
for index, row in forecast_rounds.iterrows():
    temp_df = df_scores.loc[(df_scores['origin_date'] == row['origin_date']) & \
                            (df_scores['target'] == row['target']) & \
                            (df_scores['target_end_date'] == row['target_end_date']) & \
                            (df_scores['horizon'] == row['horizon']) & \
                            (df_scores['location'] == row['location']) & \
                            (df_scores['metric'] == row['metric'])]
    
    max_value_rel, min_value_rel = np.max(temp_df["value_absolute"]), np.min(temp_df["value_absolute"])
    if len(temp_df) > 1: 
        df_scores.loc[temp_df.index, 'rank_score']  = temp_df["value_absolute"].apply(lambda x : (max_value_rel - x) / (max_value_rel - min_value_rel))

# save 
max_origin_date = df_scores.origin_date.max()
df_scores.to_csv(os.path.join(args.hub_path, f"model-evaluation/latest-forecast_scores.csv"), index=False)
df_scores.to_csv(os.path.join(args.hub_path, f"model-evaluation/snapshots/{max_origin_date}-forecast_scores.csv"), index=False)

env_file = os.getenv('GITHUB_OUTPUT')
with open(env_file, "a") as outenv:
   outenv.write (f"scoring_file_latest=model-evaluation/latest-forecast_scores.csv\n")
   outenv.write (f"scoring_file_snapshot=model-evaluation/snapshots/{max_origin_date}-forecast_scores.csv")
        
