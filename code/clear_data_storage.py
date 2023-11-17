import json
import os
import argparse
import store_changes as stc




def emptyDb(db_path):
    empty_json = dict() 

    print ("Emptying db")

    try:
        with open(db_path, 'w') as fdb:
            json.dump(empty_json, fdb, indent=4)
            print ("Emptying db - done")
    except:
        # If the file doesn't exist, handle error
        raise Exception(f"Error emptying json file: {db_path}\n")



def clearData(db_path, not_ingested):
    print (f"Clearing db {db_path}")
    
    emptyDb(db_path)

    if not_ingested:
        print ("Not ingested files present - store changes")
        print (f"Not ingested list: {not_ingested}")
        nis = ' '.join(not_ingested)
        print (f"input for store: {nis}")
        stc.store(nis)




def run(storage_type, not_ingested):
    print ("Running run")
    db_path = None   

    if storage_type == "model-output":
        db_path = os.path.join(os.getcwd(), "./repo/.github/data-storage/changes_db.json")
    
    elif storage_type == "model-metadata":
        db_path = os.path.join(os.getcwd(), "./repo/.github/data-storage/metadata_db.json")
        
    elif storage_type == "target":
        db_path = os.path.join(os.getcwd(), "./repo/.github/data-storage/target_db.json")

    else:
        print("unknown storage_type")

    if db_path is not None:
        clearData(db_path, not_ingested)


if __name__ == "__main__":

    wout = os.getenv("not_ingested")
    jdata = json.loads(wout)

    not_ingested = []

    if jdata.get('failed_ingestions') != None:
        not_ingested = jdata['failed_ingestions']
        
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--storage_type')

    args = parser.parse_args()
    print (f"storage: {args.storage_type}")

    run(args.storage_type, not_ingested)
