import json
import os

def run ():
    # get env parameters
    # Get file list
    changed_files_list = os.getenv("pr_changes", "").split(' ')
    # changed_files_list = 'model-metadata/ISI-CovTestModel.yml model-output/ISI-CovTestModel/2024-10-16-ISI-CovTestModel.csv'.split(' ')


    # Allowed folders list 
    allowed_folders = ["model-metadata/", "model-output/"]

    print(f'PR Changes list: {changed_files_list}')

    # Init list for file outside allowed folders
    files_outside_folders = []

    for file in changed_files_list:
        if not any(file.startswith(folder) for folder in allowed_folders):
            files_outside_folders.append(file)
            
    # Build fails if there are files outside allowed folders
    if files_outside_folders:
        print(f"Error: PR contains files outside the allowed folders: {files_outside_folders}")
        exit(1)
    else:
        print("All files are within the allowed folders.")


if __name__ == "__main__":
    run()