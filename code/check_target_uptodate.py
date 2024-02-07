import os
import json
import argparse
from datetime import datetime, timedelta
import pathlib


parser = argparse.ArgumentParser()
parser.add_argument('--target', default="ERVISS")
parser.add_argument('--weekday', default="Friday")
parser.add_argument('--repository', default="./repo")

args = parser.parse_args()


# list of days in a week
weekdaysList = ['Monday', 'Tuesday', 'Wednesday', 'Thursday',
                'Friday', 'Saturday', 'Sunday']

# ----
def getLastByDay  (inputDay):
    # Get today's date
    today = datetime.today()    
    # getting the last weetodaykday
    daysAgo = (today.weekday() - weekdaysList.index(inputDay)) % 7

    # Subtract the above number of days from the current date(start date)
    # to get the last week's date of the given day
    targetDate = today - timedelta(days=daysAgo)    
    return targetDate.date()


# ----
def parse_file_name(file_name):
    target = ""
    last_snapshot = None
    
    if file_name:
        target = os.path.basename(os.path.dirname(os.path.dirname(file_name)))
        print (f'Target: {target}')

        date_str = pathlib.Path(file_name).stem
        res = date_str.rsplit('-', 1)
        date_format = '%Y-%m-%d'
        last_snapshot = datetime.strptime(res[0], date_format).date()

    return target, last_snapshot



# ----
def get_updates (repository, disease_key):

    updates = {}
    j_changes = []

    try:
        target_db = os.path.join(repository, '.github/data-storage/target_db.json')
        print (f"Loading {target_db}")
        with open (target_db, "r") as j_src:
            j_data = json.load(j_src)
            j_changes = j_data[disease_key]['changes']            

    except Exception:
        print ('Load failed')


    for change in j_changes:
        target, last_snapshot = parse_file_name(change)
        updates[target] = last_snapshot

    return updates


# -------------------------------------------------
# MAIN CODE
# -------------------------------------------------

uptodate = 'false'

w_day = args.weekday
target = args.target
repo = args.repository

disease_name = os.getenv("disease_name")
if disease_name == "Influenza":
    disease_name = "ILI"

disease_k = f"{disease_name} incidence"

last_friday = getLastByDay(w_day)
print(f"last f: {last_friday}")

updates = get_updates(repository=repo, disease_key=disease_k)
print(f"Updates: {updates}")


if target in updates:
    if  updates[target] >= last_friday:
        uptodate = 'true'


# print(uptodate)
print('Pippo')

# print (f"Target uptodate = {uptodate}")

# # write to the output env
# env_file = os.getenv('GITHUB_OUTPUT')
# with open(env_file, "a") as outenv:
#    outenv.write (f"target_uptodate={uptodate}")
