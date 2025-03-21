import pandas as pd 
from datetime import datetime, timedelta
from isoweek import Week
import argparse 
import os
import numpy as np

# def get_sunday_of_week(year_week):
#     # Create a datetime object for the first day of the week
#     first_day = datetime.strptime(f'{year_week}-1', "%Y-W%W-%w")
#     # Calculate the number of days to Sunday (6 represents Sunday)
#     days_to_sunday = (6 - first_day.weekday()) % 7
#     # Add the number of days to get the Sunday of that week
#     sunday = first_day + timedelta(days=days_to_sunday)
#     return sunday


def get_sunday_of_week(year_week):
    # Create a week object for the given year week
    year, week = map(int, year_week.split('-W'))
    week_obj = Week(year, week)
    sunday = week_obj.sunday().isoformat()

    print(f"In -> {year_week}, out -> {sunday}")
    
    return sunday


def get_snapshot_date() -> str:
    
    today = datetime.now()
    days_to_last_friday = (today.weekday() - 4) % 7
    last_friday_date = today - timedelta(days=days_to_last_friday)
    return last_friday_date.date().isoformat()

parser = argparse.ArgumentParser()
parser.add_argument('--hub_path')
parser.add_argument("--indicator", help="epidemilogical indicator to be used", default="hospitaladmissions")
parser.add_argument("--pathogen", help="pathogen name", default="SARS-CoV-2")
parser.add_argument('--disease_name', default="COVID19")

# parse input arguments
args = parser.parse_args()
hub_path = str(args.hub_path)
indicator = str(args.indicator)
pathogen = str(args.pathogen)
disease_name=str(args.disease_name)

#calculate current snapshot date
snapshot_date = get_snapshot_date()

# import iso2 codes
iso_df = pd.read_csv(os.path.join(args.hub_path, "supporting-files/locations_iso2_codes.csv"))

# import erviss data
url = 'https://raw.githubusercontent.com/EU-ECDC/Respiratory_viruses_weekly_data/main/data/snapshots/{snapshot_date}_nonSentinelSeverity.csv'
url = url.format(snapshot_date = snapshot_date)
print (f'Data source: {url}')
df = pd.read_csv(url)

# filter data by pathogen
df = df.loc[df.pathogen == pathogen]
# get only age total
df = df.loc[df.age == 'total']


# format and add info on date, country
df.rename(columns={"countryname": "location_name"}, inplace=True)
df = df.merge(iso_df, on="location_name", how="left")

# drop unneeded 
df.drop(columns=["survtype", "location_name", "pathogen", "pathogentype", "age"], inplace = True)

# drop rows with empty location
df['iso2_code'] = df['iso2_code'].replace('', np.nan)
df.dropna(subset=['iso2_code'], inplace=True)

# rename iso2_code to location and yearweek to year_week
df.rename(columns={"iso2_code": "location"}, inplace=True)
df.rename(columns={"yearweek": "year_week"}, inplace=True)


# Add truth date column
df["truth_date"] = df.year_week.apply(get_sunday_of_week)


df = df.loc[df.indicator == indicator].reset_index(drop=True)
df = df[["location", "truth_date", "year_week", "value"]]

# add extra column for target
df.insert(0, 'target', 'hospital admissions')    

# save 
# Write output files
out_files_target = ['target-data/ERVISS/latest-hospital_admissions.csv', 'target-data/ERVISS/snapshots/{report_date}-hospital_admissions.csv']
out_files_target = [of.format(report_date=snapshot_date, disease_name=disease_name) for of in out_files_target]

for output_path in out_files_target:
    df.to_csv(os.path.join(args.hub_path, output_path), index=False)

env_file = os.getenv('GITHUB_OUTPUT')
with open(env_file, "a") as outenv:
   outenv.write (f"imported_files={' '.join([out_file for out_file in out_files_target if not 'latest-' in out_file] )}")







