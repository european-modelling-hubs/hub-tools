import argparse
import pandas as pd 
import pycountry
#import requests
from datetime import timedelta, datetime
import os

parser = argparse.ArgumentParser()
parser.add_argument('--hub_path')
# parser.add_argument('--disease')
parser.add_argument('--weekmin', default=202330)

args = parser.parse_args()
hub_path = str(args.hub_path)
weekmin = int(args.weekmin)
diseases = ['ILI', 'ARI']
# disease = str(args.disease)

# list of countries with flu_ID data
fluID_countries = {"ILI": ["CH", "GB-ENG", "GB-WLS", "GB-NIR", "GB-SCT"], 
                   "ARI": ["GB-ENG", "GB-WLS", "GB-NIR", "GB-SCT"]}


def import_fluID(fluid_url = "https://xmart-api-public.who.int/FLUMART/VIW_FID?$format=csv"): 
    return pd.read_csv(fluid_url)


def closest_friday():
    # Get the current date
    today = datetime.now().date()

    # Calculate the difference in days to the previous and next Friday (Friday has a weekday of 4)
    days_until_next_friday = (4 - today.weekday() + 7) % 7
    days_since_previous_friday = (today.weekday() - 4 + 7) % 7

    # Determine whether the previous or next Friday is closer
    if days_since_previous_friday <= days_until_next_friday:
        closest_friday = today - timedelta(days=days_since_previous_friday)
    else:
        closest_friday = today + timedelta(days=days_until_next_friday)

    return closest_friday


def iso3_to_iso2(iso3_code):
    try:
        if iso3_code == "X09": 
             return "GB-ENG"
        elif iso3_code == "X10": 
             return "GB-WLS"
        elif iso3_code == "X11":
             return "GB-NIR"
        elif iso3_code == "X12":
             return "GB-SCT"
        else:
            country = pycountry.countries.get(alpha_3=iso3_code)
            iso2_code = country.alpha_2
            return iso2_code
    except AttributeError:
        return None
    

def parse_week(ISOYW): 
    year, week = str(ISOYW)[:4], str(ISOYW)[4:]
    return year + "-W" + week


def get_location_data(df_fluid, location, weekmin, disease, age_grp = "All"): 
        
    # select data for country
    df_fluid_location = df_fluid.loc[df_fluid.location == location]

    # select data after min week 
    df_fluid_location = df_fluid_location.loc[df_fluid_location.ISOYW >= weekmin]

    # select age group 
    df_fluid_location = df_fluid_location.loc[df_fluid_location.AGEGROUP_CODE == age_grp]
    
    # select disease
    df_fluid_location = df_fluid_location.loc[df_fluid_location.CASE_INFO == disease]
    
    
    df_fluid_location = df_fluid_location[["location", "ISOYW", "ISO_WEEKSTARTDATE", "REPORTED_CASES", "POP_COV"]]

    df_fluid_location.sort_values(by="ISOYW", ignore_index=True, inplace=True)

    # compute incidence 
    df_fluid_location["value"] = df_fluid_location["REPORTED_CASES"] / df_fluid_location["POP_COV"] * 100000

    # format 
    df_fluid_location["truth_date"] = pd.to_datetime(df_fluid_location["ISO_WEEKSTARTDATE"]) + timedelta(days=6)
    df_fluid_location["year_week"] = df_fluid_location.ISOYW.apply(parse_week)
    
    return df_fluid_location[["location", "truth_date", "year_week", "value"]]


# import raw data 
df_fluid = import_fluID()
print("Max Week:", df_fluid.ISOYW.max())

# add iso 2
df_fluid["location"] = df_fluid["COUNTRY_CODE"].apply(iso3_to_iso2)

# keep a list of imported file to be returned to the caller ()
imported = []

for disease in diseases:
    print (f"updating {disease}")

    # parse data 
    df_final = pd.DataFrame() 

    for country in fluID_countries[disease]:
        df_country = get_location_data(df_fluid, country, weekmin, disease, age_grp = "All")
        df_final = pd.concat((df_final, df_country), ignore_index=True)

    # check that the df_final contains new data
    max_date_new = df_final.year_week.max()

    # get latest imported date
    latest_import_file = os.path.join(args.hub_path, f"target-data/FluID/latest-{disease}_incidence.csv")
    max_date_old = pd.read_csv(latest_import_file).year_week.max() if os.path.exists(latest_import_file) else parse_week(weekmin)

    if max_date_new != max_date_old:
        # save
        df_final.to_csv(os.path.join(args.hub_path, f"target-data/FluID/latest-{disease}_incidence.csv"), index=False)
        
        snapshot_filename = closest_friday().strftime("%Y-%m-%d") + f"-{disease}_incidence.csv"

        print(f'Snapshot file name: {snapshot_filename}')

        out_snapshot_file = "target-data/FluID/snapshots/{snap_file}"        
        out_snapshot_file.format(snap_file = snapshot_filename)

        print (f'Output file name: {out_snapshot_file}')

        df_final.to_csv(os.path.join(args.hub_path, out_snapshot_file), index=False)

        imported.append(out_snapshot_file)

    else:
        print(f"Nothing to update: new: {max_date_new}, old: {max_date_old}")


print (f'Imported list: {imported}')

env_file = os.getenv('GITHUB_OUTPUT')
with open(env_file, "a") as outenv:
    outenv.write (f"imported_snapshot={' '.join(imported)}")

print(f'Updates completed')