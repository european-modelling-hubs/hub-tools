import os
import sys
import csv
from urllib.request import urlopen
import urllib.error
from datetime import datetime, timedelta, timezone
from isoweek import Week
import argparse


import pycountry



def get_snapshot_date() -> str:
    
    today = datetime.now(timezone.utc)
    days_to_last_friday = (today.weekday() - 4) % 7
    last_friday_date = today - timedelta(days=days_to_last_friday)
    return last_friday_date.date().isoformat()



# Config

parser = argparse.ArgumentParser()
parser.add_argument('--hub_path')


def snapshot_date_arg(value):
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Expected snapshot date YYYY-MM-DD") from exc
    if parsed.isoformat() != value or parsed.weekday() != 4:
        raise argparse.ArgumentTypeError("Snapshot date must be a Friday in YYYY-MM-DD format")
    return value


parser.add_argument('--snapshot_date', type=snapshot_date_arg,
                    help='ERVISS Friday snapshot; defaults to the last Friday (UTC)')

args = parser.parse_args()

hub_path            = str(args.hub_path)
diseases_list       = ["ILI", "ARI"]
target_records_list = [[('target', 'location', 'truth_date', 'year_week', 'value')] for _ in range(len(diseases_list))]
countries_to_x1000  = ('Malta', 'Luxembourg', 'Cyprus') # Values for this countries must be multiplied by 1000
snapshot_date       = args.snapshot_date or get_snapshot_date()

# Build URL
url = 'https://raw.githubusercontent.com/EU-ECDC/Respiratory_viruses_weekly_data/main/data/snapshots/{snapshot_date}_ILIARIRates.csv'
url = url.format(snapshot_date = snapshot_date)

# Read data from URL
print (f'Reading from Http url {url}')

try:
    response = urlopen(url, timeout=60)

except urllib.error.HTTPError:
    print (f'Http error - failed to access url {url}')
    sys.exit(1)


lines = [line.decode('utf-8') for line in response.readlines()]
csv_reader = csv.DictReader(lines, delimiter=',')

for row in csv_reader:
    row_indicator = row['indicator'][:3]
    
    if (row['survtype'] != 'primary care syndromic'
            or row_indicator not in diseases_list
            or row['age'] != 'total'
       ):
        continue

    target = row_indicator + ' incidence'
    country2 = pycountry.countries.lookup(row['countryname']).alpha_2
    year, week = map(int, row['yearweek'].split('-W'))
    week_obj = Week(year, week)
    truth_date = week_obj.sunday().isoformat()
    value = float(row['value'])
    if row['countryname'] in countries_to_x1000:
        value = value*1000

    record = (target, country2, truth_date, row['yearweek'], value)
    target_records_list[diseases_list.index(row_indicator)].append(record)

imported = []

for disease_name in diseases_list:
    # Write output files
    out_files_target = ['target-data/ERVISS/latest-{disease_name}_incidence.csv', 'target-data/ERVISS/snapshots/{report_date}-{disease_name}_incidence.csv']
    out_files_target = [of.format(report_date=snapshot_date, disease_name=disease_name) for of in out_files_target]

    for output_path in out_files_target:
        with open( os.path.join(args.hub_path, output_path), 'w') as output_file:
            csv_writer = csv.writer(output_file)
            csv_writer.writerows(target_records_list[diseases_list.index(disease_name)])

        if not 'latest-' in output_path:
            imported.append(output_path) 


print (f'Imported list: {imported}')

env_file = os.getenv('GITHUB_OUTPUT')
if env_file:
    with open(env_file, "a") as outenv:
        outenv.write(f"imported_files={' '.join(imported)}\n")
