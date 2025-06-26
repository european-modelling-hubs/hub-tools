import os
import csv


def get_latest_origin_dates(repo_path):
    """ 
    Estrae gli origin_date dei record in cui is_latest è True dal file CSV specificato. 
    """ 
    filepath = os.path.join(repo_path, 'supporting-files/forecasting_weeks.csv')
    origin_dates = set() 

    with open(filepath, newline='', encoding='utf-8') as csvfile: 
        reader = csv.DictReader(csvfile) 
        for row in reader: 
            if row.get("is_latest", "").strip().lower() == "true": 
                origin_dates.add(row.get("origin_date")) 

    return list(origin_dates)[0] 
