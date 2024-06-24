import os
import json

"""
Persists changes in the target-data list to the specific storage target_db.json
"""
def storeTargetData (target_data):
    # get the target name from path 
    
    target_name = os.path.splitext(os.path.basename(target_data[0]))[0].split('-')[-1]

    out_data = {}    
    out_data['target'] = target_name.replace("_", " ") 
    out_data['changes'] = target_data
        
    if out_data["changes"]:
        db_path = os.path.join(os.getcwd(), "./repo/.github/data-storage/target_db.json")
        print(f"DB path: {db_path}")
        updateTargetJson(db_path, out_data)


"""
Format record for each target truth and store in the target_db.json
"""
def updateTargetJson (json_file_path, out_data):

    json_data = None
    target = out_data.get("target")
    
    # Step 1: Read the existing data from the JSON file
    try:
        with open (json_file_path, 'r') as fdb:
            json_data = json.load(fdb)            
    except FileNotFoundError:
        # If the file doesn't exist, handle error
        raise Exception(f"Json file not found {json_file_path}\n")

    if target not in json_data:
        json_data[target] = {'changes': out_data['changes']}
    else:
        json_data[target]['changes'] = list(set(json_data[target]['changes'] + out_data['changes']))
    
    try:
        with open(json_file_path, 'w') as fdb:
            json.dump(json_data, fdb, indent=4)
    except:
        # If the file doesn't exist, handle error
        raise Exception(f"Error writing  {json_data} \n to json file: {json_file_path}\n")    


"""
Format record for each projection and store in the projections_db.json
"""
def storeProjections (projections, isEnsemble = False):

    team = os.path.basename(os.path.split(projections[0])[0]).split('-')[0]
    if not team:
     raise Exception(f"invalid input data  {projections}\n")

    out_data = {}    
    out_data['team'] = team
    out_data['models'] = []

    for projection in projections:

        # get the model name from path
        model = tuple(os.path.basename(os.path.split(projection)[0]).split('-'))[1]

        model_entry = next((item for item in out_data['models'] if item["model"] == model), None)
        if model_entry is None:
            out_data['models'].append({"model" : model, "changes": [projection]})
        else:
            model_entry["changes"].append(projection)

    if out_data['models']:        
        db_path = os.path.join(os.getcwd(), "repo/.github/data-storage" + os.path.sep + ("ensemble_db.json" if isEnsemble else "projections_db.json"))
        print(f"DB path: {db_path}")
        print(f"saving data: {out_data}")
        updateForecastsJson(db_path, out_data)
    


"""
update model-output 
"""
def updateForecastsJson(json_file_path, changes):

    json_data = None

    team = changes.get("team")
    n_entries = changes.get("models")

    # Step 1: Read the existing data from the JSON file
    try:
        with open (json_file_path, 'r') as fdb:
            json_data = json.load(fdb)            
    except FileNotFoundError:
        # If the file doesn't exist, handle error
        raise Exception(f"Json file not found {json_file_path}\n")

    # Check if the "team" key exists and is a list
    if team not in json_data:
        # if brand new, just save commits
        json_data[team] = n_entries

    else:
        #get the list of previous saved data for this team
        j_records = json_data[team]

        for entry in n_entries:
                
            j_model = [j_record for j_record in j_records if j_record.get("model") == entry.get("model")]
            if j_model == [] :
                j_records.append(entry)
            else:
                j_model[0]["changes"] += set(entry["changes"]).difference (j_model[0]["changes"])

    try:
        with open(json_file_path, 'w') as fdb:
            json.dump(json_data, fdb, indent=4)
    except:
        # If the file doesn't exist, handle error
        raise Exception(f"Error writing  {json_data} \n to json file: {json_file_path}\n")
        

"""
Function 
"""
def storeStdData (data, db_file):
    
    print ("Storing data")    
    db_path = os.path.join(os.getcwd(), "./repo/.github/data-storage/", db_file)
    
    print(f"DB path: {db_path}")
    updateJsonData(db_path, data)


def updateJsonData (json_file_path, changes):

    json_data = None

    # Step 1: Read the existing data from the JSON file
    try:
        with open (json_file_path, 'r') as fdb:
            json_data = json.load(fdb)
            print(f"JSON DB CONTENT: \n{json_data}")
            
    except FileNotFoundError:
        # If the file doesn't exist, handle error
        raise Exception(f"Json file not found {json_file_path}\n")

    json_data["changes"] = changes if "changes" not in json_data else list(set(json_data["changes"] + changes))

    try:
        with open(json_file_path, 'w') as fdb:
            json.dump(json_data, fdb, indent=4)
    except:
        # If the file doesn't exist, handle error
        raise Exception(f"Error writing  {json_data} \n to json file: {json_file_path}\n")




"""
The store functions get the list of changes and, based on path and file-type stores
the changes in the corresponding json db
input: blac
"""
def store(changes_list):

    # Make a list out of the changed files
    changes = changes_list.split(" ")

    # List should not be empty
    if not changes:
        raise Exception(f"Empty commit")
    

    model_changes = []
    metadata_changes = []
    targetdata_changes = []

    
    # 
    for change in changes:
                
        # needed for different deepness of paths
        if change.startswith("model-output"):
            # save model output
            model_changes.append(change)
        elif change.startswith("model-metadata"):
            # save meta-data
            metadata_changes.append(change)
        elif change.startswith("target-data") and not 'latest-' in change:
            # save target-data
            targetdata_changes.append(change)
        else :
            # unknown just discard
            print (f'Unkown file submitted {change}! Skip it')


    if model_changes:
        print (f"{len(model_changes)} changes in model-output")
        storeProjections(model_changes)
    
    if metadata_changes:
        print (f"{len(metadata_changes)} changes in model-metadata")
        storeStdData(metadata_changes, "metadata_db.json")

    if targetdata_changes:
        print (f"{len(targetdata_changes)} changes in targetdata")
        storeTargetData(targetdata_changes)




if __name__ == "__main__":

    """
    Get the list of changes within the PR in the 'data' environment variable
    Data to be stored are then passed to the store_data function
    """

    changes_list = os.getenv("data")        
    store(changes_list)
