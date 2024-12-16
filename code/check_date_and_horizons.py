import pandas as pd
import os
import argparse



parser = argparse.ArgumentParser()
parser.add_argument('--hub_path', default = './')
parser.add_argument('--forecasting_file', default="supporting-files/forecasting_weeks.csv")




# Verify target_end_dates are all in the correct forecasting week
def verifiy_forecasting_dates(df: pd.DataFrame, fw_file: str ) -> None:

        
    if not os.path.exists(fw_file):
        print (f'Forecasting weeks csv file not found: {fw_file}. Aborting')
        exit(1)

    # get current forecasting weeks 
    forecasting_weeks = pd.read_csv(fw_file)
    forecasting_weeks = forecasting_weeks.loc[forecasting_weeks.is_latest == True]

    df_model_h = df[["horizon", "target_end_date"]].drop_duplicates()

    for _, row in df_model_h.iterrows():

        horizon = row.horizon
        target_end_date = row.target_end_date
        correct_target_end_date = forecasting_weeks.loc[forecasting_weeks.horizon == horizon].target_end_date.values[0]

        if target_end_date != correct_target_end_date: 
            print(f"Wrong target_end_date found for horizon {horizon}: {target_end_date} instead of {correct_target_end_date}")
            exit(1)

    
    print('Target_end_dates verified')



# get input
args = parser.parse_args()
hub_path = str(args.hub_path)
forecasting_file = str(args.forecasting_file)

# get list of model-output files from environment
input_list = os.getenv("pr_changes", "").split(' ')

# get current forecasting weeks
forecasting_weeks_path = os.path.join(hub_path, forecasting_file)


for model in input_list: 
    
    print(f'Validating input model: {model}')

    # unpacking the tuple
    file_name, file_extension = os.path.splitext(model)

    if model.startswith('model-output') and file_extension == '.csv':

        df = pd.read_csv(os.path.join(hub_path, model))        
        verifiy_forecasting_dates(df = df, fw_file = forecasting_weeks_path)
        
        print ('Model check OK')
