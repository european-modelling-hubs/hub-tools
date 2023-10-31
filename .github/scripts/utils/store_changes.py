import os
import json


# Update is executed for one team at a time
def update_json_db (json_file_path, changes):

  json_data = None

  team = changes.get("team")
  n_entries = changes.get("models")
  
  if not team:
     raise Exception(f"invalid input data  {changes}\n")
  

  # Step 1: Read the existing data from the JSON file
  try:
    with open (json_file_path, 'r') as fdb:
      json_data = json.load(fdb)
      print(f"JSON CONTENT: \n{json_data}")
    
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
        print("Add new team to the backup")
      else:
        j_model[0]["changes"] += set(entry["changes"]).difference (j_model[0]["changes"])

  print(f"Saving json: \n{json_data}")

  with open(json_file_path, 'w') as fdb:
    json.dump(json_data, fdb, indent=4)

  print(f"JOB DONE >>>>>>>>")



# Main
if __name__ == "__main__":

  # Config
  db_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "changes_db.json")

  updated_json_data = os.getenv("data")
  print ("### Data: {}".format(updated_json_data))
  
  jobj = json.loads(updated_json_data)
  update_json_db (db_path, jobj)
