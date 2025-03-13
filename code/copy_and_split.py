import os
import csv
import json
import shutil
import argparse 


def copy_and_split_csv(file_path, temp_dir, max_records=25000):
    """Copies CSV to temp folder, splits it into parts, and returns part file paths."""
    base_name = os.path.basename(file_path)
    temp_file_path = os.path.join(temp_dir, base_name)
    
    # Copy original CSV to temp directory
    shutil.copy(file_path, temp_file_path)
    
    # Read CSV and split into chunks
    part_files = []
    
    with open(temp_file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        rows = list(reader)
    
    num_parts = (len(rows) // max_records) + (1 if len(rows) % max_records > 0 else 0)
    
    for i in range(num_parts):
        part_file_name = f"{base_name.replace('.csv', '')}_part_{i}.csv"
        part_file_path = os.path.join(temp_dir, part_file_name)
        with open(part_file_path, mode='w', newline='', encoding='utf-8') as part_file:
            writer = csv.writer(part_file)
            writer.writerow(header)
            writer.writerows(rows[i * max_records: (i + 1) * max_records])
        part_files.append(part_file_path)

    # remove original file once split is completed 
    os.remove(temp_file_path)
    
    return part_files

def extract_paths_from_subfolder(paths, subfolder):
    extracted_paths = []
    
    for path in paths:
        parts = path.split(os.sep)  # Divide il path in parti usando il separatore di sistema
        
        if subfolder in parts:
            index = parts.index(subfolder)  # Trova la posizione del subfolder
            extracted_path = os.path.join(*parts[index:])  # Ricostruisce il path relativo
            extracted_paths.append(extracted_path)
    
    return extracted_paths


def update_json_changes(in_list, filename="changes.json"):
    # Se il file esiste, caricare i dati esistenti, altrimenti inizializzare con un dizionario vuoto
    if os.path.exists(filename):
        with open(filename, "r") as json_file:
            try:
                data = json.load(json_file)
            except json.JSONDecodeError:
                data = {}
    else:
        data = {}
    
    # Assicurarsi che "changes" sia una lista
    if "changes" not in data:
        data["changes"] = []
    
    # Aggiungere nuovi elementi evitando duplicati
    data["changes"] = list(set(data["changes"] + in_list))
    
    # Scrivere i dati aggiornati nel file JSON
    with open(filename, "w") as json_file:
        json.dump(data, json_file, indent=4)



# input parameters
# hub_path
# json_data
def main (hub_path, tmp_dir, json_data):

    print (f'>>>> DEBUG <<<<< \nHub-path: {hub_path},\nTemporary folder: {tmp_dir}, \nData: {json_data}') 

    changes = json_data.get("changes", [])
    tmp_dir_abs = os.path.join(hub_path, tmp_dir)

    # build temp folder
    os.makedirs(tmp_dir_abs, exist_ok=True)

    # create temporary json file with all the files to be uploaded
    jdb = os.path.join(tmp_dir_abs, "changes.json")

    for change in changes:
        # compose absolute file name
        fname = os.path.join(hub_path, change)
        parts_list = copy_and_split_csv (fname, tmp_dir_abs)
        parts_list = extract_paths_from_subfolder(parts_list, tmp_dir.split(os.sep)[0])
        print (parts_list)
        update_json_changes(parts_list, filename= jdb)

        
if __name__ == "__main__":
  
    # Arguments 
    parser = argparse.ArgumentParser()
    parser.add_argument('--hub_path', default="./repo")
    parser.add_argument('--tmp_folder', default="./github/tmp")

    args = parser.parse_args()

    hub_path = str(args.hub_path)
    tmp_folder = str(args.tmp_folder)

    # Env parameters
    json_data = os.getenv("data")
    # json_data = json_data.replace("\'", "")
        
    main(hub_path = hub_path, tmp_dir = tmp_folder,  json_data = json.loads(json_data))
