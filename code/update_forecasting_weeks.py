from datetime import datetime, timedelta
import pandas as pd 
import os
import argparse

# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('--hub_path')
args = parser.parse_args()

def parse_date(date_str): 
    year, month, day = date_str.split("-")[0], date_str.split("-")[1], date_str.split("-")[2]
    return datetime(year=int(year), month=int(month), day=int(day))

# read current forecasting weeks file
forecasting_weeks = pd.read_csv(os.path.join(args.hub_path, "supporting-files/forecasting_weeks.csv"))
forecasting_weeks_last = forecasting_weeks.loc[forecasting_weeks.is_latest == True]
forecasting_weeks_last.sort_values(by="horizon", inplace=True, ignore_index=True)

# update forecasting weeks
forecasting_weeks_new = pd.DataFrame(data={
    "horizon": [1,2,3,4],
    "target_end_date": [parse_date(el) + timedelta(days=7) for el in forecasting_weeks_last.target_end_date.values]})
forecasting_weeks_new.insert(0, "origin_date", parse_date(forecasting_weeks_last.origin_date.values[0]) + timedelta(days=7))
forecasting_weeks_new.insert(forecasting_weeks_new.shape[0] - 1, "submission_round", forecasting_weeks_last.submission_round.values[0] + 1)
forecasting_weeks_new.insert(forecasting_weeks_new.shape[0] - 1, "is_latest", True)

forecasting_weeks_new.origin_date = forecasting_weeks_new.origin_date.astype(str)
forecasting_weeks_new.target_end_date = forecasting_weeks_new.target_end_date.astype(str)

forecasting_weeks["is_latest"] = False
forecasting_weeks = pd.concat((forecasting_weeks_new, forecasting_weeks))

# write file
forecasting_weeks.to_csv(os.path.join(args.hub_path, "supporting-files/forecasting_weeks.csv"), index=False)
