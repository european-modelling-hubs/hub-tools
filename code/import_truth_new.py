import os
import sys
import csv
from urllib.request import urlopen
import urllib.error
from datetime import datetime, timedelta
from isoweek import Week
import argparse


import pycountry

# Config

parser = argparse.ArgumentParser()
parser.add_argument('--hub_path')
parser.add_argument('--disease_name', default="ILI")

args = parser.parse_args()

hub_path=str(args.hub_path)
disease_name=str(args.disease_name)

url = 'https://raw.githubusercontent.com/EU-ECDC/Respiratory_viruses_weekly_data/main/data/snapshots/{snapshot_date}_ILIARIRates.csv'
out_files_target = ['target-data/ERVISS/latest_new-{disease_name}_incidence.csv', 'target-data/ERVISS/snapshots/{report_date}-{disease_name}_incidence_new.csv']

countries_to_x1000 = ('Malta', 'Luxembourg', 'Cyprus') # Values for this countries must be multiplied by 1000

# Build URL

today = datetime.now()
days_to_last_friday = (today.weekday() - 4) % 7
last_friday_date = today - timedelta(days=days_to_last_friday)
snapshot_date=last_friday_date.date().isoformat()

url = url.format(snapshot_date=snapshot_date)
try:
    response = urlopen(url)
except urllib.error.HTTPError:
    print (f'Http error - failed to access url {url}')
    sys.exit(1)


# Read data
print (f'Reading from Http url {url}')

lines = [line.decode('utf-8') for line in response.readlines()]

csv_reader = csv.DictReader(lines, delimiter=',')

target_records = [('location', 'truth_date', 'year_week', 'value')]

for row in csv_reader:
    if (row['survtype'] != 'primary care syndromic'
            or row['indicator'] != f'{disease_name}consultationrate'
            or row['age'] != 'total'
       ):
        continue
    country2 = pycountry.countries.lookup(row['countryname']).alpha_2
    year, week = map(int, row['yearweek'].split('-W'))
    week_obj = Week(year, week)
    truth_date = week_obj.sunday().isoformat()
    value = float(row['value'])
    if row['countryname'] in countries_to_x1000:
        value = value*1000
    target_records.append((country2, truth_date, row['yearweek'], value))


# Write output files

out_files_target = [of.format(report_date=snapshot_date, disease_name=disease_name) for of in out_files_target]

for output_path in out_files_target:
    with open( os.path.join(args.hub_path, output_path), 'w') as output_file:
        csv_writer = csv.writer(output_file)
        csv_writer.writerows(target_records)    

env_file = os.getenv('GITHUB_OUTPUT')
with open(env_file, "a") as outenv:
   outenv.write (f"imported_files={' '.join([out_file for out_file in out_files_target if not 'latest-' in out_file] )}")
