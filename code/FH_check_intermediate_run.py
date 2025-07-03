import os
import csv
import json
import yaml
import argparse 

from FH_utils import get_latest_origin_dates

def new_models_number (repo_path):
    db_path = os.path.join(repo_path, '.github/data-storage/changes_db.json')

    metadata_folder =  os.path.join(repo_path, 'model-metadata/')

    changes = load_changes_db(db_path)
    models_count = count_submitted_models(changes, metadata_folder)
    return models_count
    
    # return True if count_submitted_models(changes, metadata_folder) >= 3 else False


def suitable_for_ensemble (repo_path):

    filename = f"{get_latest_origin_dates(repo_path)}-respicast-hubEnsemble-ensemble_models.json"

    file_path = os.path.join(repo_path, f".github/logs/{filename}")
    return True if max_model_count_from_file(file_path) >= 3 else False



def max_model_count_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return 0

    max_models = 0

    
    for target in data:
        for country in target.get("countries", []):
            for member in country.get("members", []):
                models = member.get("models", [])
                max_models = max(max_models, len(models))

    return max_models



def load_changes_db(filepath):
    """
    Carica il file JSON contenente le informazioni sui cambiamenti dei modelli.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def get_team_model_metadata(team, model, metadata_folder):
    """
    Legge il file YAML corrispondente a <team>-<model>.yml e restituisce il valore del campo
    'team_model_designation'.
    """
    filename = f"{team}-{model}.yml"
    filepath = os.path.join(metadata_folder, filename)

    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"File di metadata non trovato: {filepath}")

    with open(filepath, 'r', encoding='utf-8') as f:
        metadata = yaml.safe_load(f)

    return metadata.get("team_model_designation", "").lower()

def count_submitted_models(changes_db, metadata_folder):
    """
    Conta quanti modelli hanno un 'team_model_designation' pari a 'primary' o 'secondary'.
    """
    submitted_models_count = 0

    for team, models in changes_db.items():
        for model_entry in models:
            model_name = model_entry.get("model")
            if not model_name:
                continue  # Skippa se manca il nome del modello

            try:
                designation = get_team_model_metadata(team, model_name, metadata_folder)
                if designation in {"primary", "secondary"}:
                    print (f"Model name: {model_name} is primary or secondary")
                    submitted_models_count += 1
                else:
                    print (f"Model name: {model_name} is not primary or secondary")
            except FileNotFoundError as e:
                print(f"[AVVISO] {e}")
                continue  # Se manca il file, prosegue con gli altri

    return submitted_models_count


if __name__ == "__main__":
    print ('RespiCastUtils')

    # Arguments 
    parser = argparse.ArgumentParser()
    parser.add_argument('--hub_path', default="./repo")
    
    args = parser.parse_args()

    hub_path = str(args.hub_path)

    # ---------
    new_models = new_models_number(hub_path)
    
    if  new_models >= 3 or (new_models > 0 and suitable_for_ensemble(hub_path)):
        print ("sutiable")
        exit(0)
    # ---------
    
    # if new_models_number(hub_path) or suitable_for_ensemble(hub_path):
    #     print ("sutiable")
    #     exit(0)

    print ("not suitable")
    exit(1)
