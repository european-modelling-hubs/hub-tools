import csv
import pathlib
import os
import argparse

# if file_name  env must also 

parser = argparse.ArgumentParser()
parser.add_argument('--repository', default="./repo")
parser.add_argument('--root', default="target-data")
parser.add_argument('--source_list', default="ERVISS")


args = parser.parse_args()


def merge_csv_files(files, output_file):
    # List to store all CSV file rows
    all_rows = []
    file_header = ''

    # loop over files list
    for file in files:
        # Check if file exists
        if os.path.exists(file):
            # open CSV file and read rows
            with open(file, 'r', newline='') as csv_file:
                reader = csv.reader(csv_file)                    
                header = next(reader, None)  # Leggi l'intestazione se presente
                
                file_path = pathlib.PurePath(file)
                source = file_path.parent.name

                if not file_header:
                    file_header = header
                    
                for row in reader:
                    # Append file folder as last column
                    row.append(source)
                    all_rows.append(row)
        else:
            print(f"File '{file}' not found. Skip!")

    if not all_rows:
        raise Exception(f"Source files are all empty\n")
    
    # Save all the rows to a new CSV file
    with open(output_file, 'w', newline='') as csv_output:
        writer = csv.writer(csv_output)
        # write header
        if file_header:
            file_header.append('data_source')
            writer.writerow(file_header)
        
        # and all the rows
        writer.writerows(all_rows)

    print(f"Job done. Merged data saved to '{output_file}'.")


# repo -> the folder where the checked out repo resides 
# root_folder -> the name of the surveillance folder
# merge_list -> the list of targets we want to be merged
# file_name -> the file name to be merged. If empty, "latest-DISEASENAME_incidence.csv" is used
def do_the_merge (repo, root_folder, merge_list, file_name):

    sources = []

    # target container will be named as mergin files and will be stored directly under the root folder
    output_container = os.path.join(repo, root_folder,  file_name)

    # build the merging files list
    for merge_folder in merge_list:
        # compose a complete path for each source file
        source = os.path.join(repo, root_folder, merge_folder, file_name)
        sources.append(source)
    
    if sources:
        merge_csv_files(files=sources, output_file=output_container)
    else:
      raise Exception(f"Can not do the merge, source list is empty\n")

    print("do_the_merge completed")


# get the disease name by the env since it is a repo var
# this is used to compose the file_name to merge from given folders

file_names = []

repo_disease_name = os.getenv("disease_name")

if repo_disease_name == "Syndromic_indicator":
    file_names = ["latest-ILI_incidence.csv", "latest-ARI_incidence.csv"]
elif repo_disease_name == "COVID-19":
    file_names = ["latest-hospital_admissions.csv"]
else:
    print (f'Unknow repository disease name for merge: {repo_disease_name}')
    exit(1)


for f_name in file_names:
    sources = args.source_list.split(" ")
    do_the_merge(repo=args.repository, root_folder=args.root, merge_list=sources, file_name = f_name)

print("All the recents merged successfully")
