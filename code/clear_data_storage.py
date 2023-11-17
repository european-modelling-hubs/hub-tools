import json
import os
import argparse
import store_changes as stc




def emptyDb(db_path):
    empty_json = dict() 
    #emtpy_json_s = json.dumps(empty_json)

    try:
        with open(db_path, 'w') as fdb:
            json.dump(empty_json, fdb, indent=4)
    except:
        # If the file doesn't exist, handle error
        raise Exception(f"Error emptying json file: {db_path}\n")



def clearData(db_path, not_ingested):
    print ("Clearing model ouput")
    db_path = os.path.join(os.getcwd(), "./repo/.github/data-storage/changes_db.json")
    emptyDb(db_path)

    if not_ingested:
        stc.store(not_ingested)




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

    parser = argparse.ArgumentParser()
    parser.add_argument('--storage_type')
    parser.add_argument('--not_ingested', default=[])

    args = parser.parse_args()
    
    print (f"storage: {args.storage_type}")
    print (f"not ingested: {args.not_ingested}")

    run(args.storage_type, args.not_ingested)
