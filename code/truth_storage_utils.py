import argparse
import os
import json
from datetime import datetime

DELTA_DAYS = 3

#
#
def read_jbd():
    db_path = os.path.join(os.getcwd(), "./repo/.github/data-storage/import_truth_db.json")
    j_data = {}

     # Step 1: Read the existing data from the JSON file
    try:
        with open (db_path, 'r') as fdb:
            j_data = json.load(fdb)            

    except FileNotFoundError:
        # If the file doesn't exist, handle error
        raise Exception(f"Json db file not found {db_path}\n")

    return j_data

#
#
def write_jdb(j_data):
    db_path = os.path.join(os.getcwd(), "./repo/.github/data-storage/import_truth_db.json")

    try:
        with open(db_path, 'w') as fdb:
            json.dump(j_data, fdb, indent=4)
    except:
        # If the file doesn't exist, handle error
        raise Exception(f"Error writing  {j_data} \n to json file: {db_path}\n")


#
#
def persist_import(source, reference):
    print ('saving import to json db')
        
    update = {} 
    update['last_import'] = datetime.now().strftime("%Y-%m-%d") # current date and time
    update['reference_date'] = reference

    json_data = read_jbd()

    json_data[source] = update    
    write_jdb (json_data)

#
#
def is_out_of_date (last_update):

    out_of_date = True
 
    try: 
        delta = datetime.now() - datetime.strptime(last_update, "%Y-%m-%d")
        if delta < DELTA_DAYS:
            out_of_date = False
    except ValueError:
        print (f'Invalid update datetime {last_update}')
        
    return out_of_date

#
#
def check_last_run (source):
    
    monday_run = False

    json_data = read_jbd()
    if not source in json_data.keys() or is_out_of_date(json_data[source]['last_import']):
        monday_run = True
    
    env_file = os.getenv('GITHUB_OUTPUT')
    with open(env_file, "a") as outenv:
        outenv.write (f"run_on_monday={ monday_run }")


#
#
def run (action, source, reference):

    if action == 'save':
        persist_import (source, reference)
    else: 
        check_last_run(source)



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--action', default="check")
    parser.add_argument('--source', default="ERVISS")
    parser.add_argument('--reference_date', default="")

    args = parser.parse_args()

    action = str(args.action)
    source = str(args.source)
    ref_date = str(args.reference_date)

    run (action, source, ref_date)