import os
import sys
import yaml
import argparse

def extract_emails_from_yml(folder_path, excluded_files):
    emails = set() # use a set to avoid duplicates

    for filename in os.listdir(folder_path):
        if not filename.endswith('.yml') or filename in excluded_files:
            continue

        file_path = os.path.join(folder_path, filename)
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                content = yaml.safe_load(file)
                contributors = content.get('model_contributors', [])
                for contributor in contributors:
                    email = contributor.get('email')
                    if email:
                        emails.add(email.strip())
        except Exception as e:
            print(f"Errore nella lettura del file {filename}: {e}")

    return sorted(emails)

if __name__ == "__main__":

    env_file = os.getenv('GITHUB_OUTPUT')
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder_path')
    parser.add_argument('--exclusions', default='')

    args = parser.parse_args()

    print (f"Folder: {args.folder_path},\nexclusions: {args.exclusions}")

    folder = args.folder_path
    excluded = args.exclusions
    

    emails = extract_emails_from_yml(folder, excluded.split())
    print("\n\nExtracted: \n".join(emails))

    print(f"\nTotale email uniche trovate: {len(emails)}")

    with open(env_file, "a") as outenv:
        outenv.write (f"email_list={','.join(emails)}")
