import os
import json
import argparse
import pandas as pd
from itertools import product
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--configfile', default='hub-config/tasks.json')
# parser.add_argument('-i', '--input', default = '')
parser.add_argument('-t', '--taskids', default='target_end_date pop_group')

args = parser.parse_args()


def validate_parquet_file(all_data, group_fields, validation_rules):
    """
    Validates a single parquet file based on the specified rules.
    
    Parameters:
    - file_path (str): Path to the parquet file.
    - group_fields (list of str): List of fields to group by (N1 fields).
    - validation_rules (dict): A dictionary where keys are field names (N2 fields)
                               and values are lists of admitted values for each field.
    
    Returns:
    - validation_results (dict): A dictionary containing the validation status for each combination
                                 of group fields. The key is a tuple of group field values, and the
                                 value is either True (valid) or a list of missing combinations.
    """
        
    # Get the N2 fields that need to be validated
    validation_fields = list(validation_rules.keys())
    
    # Initialize the dictionary to store validation results
    validation_results = {}
    
    # Group by the N1 fields
    grouped = all_data.groupby(group_fields)
    
    for group_values, group_df in grouped:
        # Get the unique combinations of the N2 fields present in the group
        existing_combinations = set(tuple(x) for x in group_df[validation_fields].drop_duplicates().values)
        
        # Get all possible combinations based on the validation rules
        expected_combinations = set(product(*[validation_rules[field] for field in validation_fields]))
        
        # Check for missing combinations
        missing_combinations = expected_combinations - existing_combinations
        
        # Store the result in the dictionary
        if missing_combinations:
            validation_results[group_values] = list(missing_combinations)
        else:
            validation_results[group_values] = True
        
    return validation_results


def read_hub_config (config_file: str, round_id: str) -> tuple[list[str], list[str], list[int]]:
    pop_groups = []
    horizons = []
    target_end_dates = []

    # Open and read the JSON file
    with open(config_file, 'r') as jconfig:
        print(f'Reading config file [{config_file}]...')
        jdata = json.load(jconfig)

        print('Loop over rounds...')
        
        for round in jdata['rounds']:
            # get the var used as round_id
            # and read the value (it is the same for all the model tasks in the same round)
            r_id_var = round['round_id']
            task_ids = round['model_tasks'][0]['task_ids']
            r_id = task_ids[r_id_var]['required'][0]

            # check round id
            if round_id == r_id:            
                pop_groups = task_ids['pop_group']['optional']
                target_end_dates = task_ids['target_end_date']['optional']                
                horizons = task_ids['horizon']['optional']
            else:
                continue

    return (pop_groups, target_end_dates, horizons)
            

def check_task_ids (src_file: str, in_tasks: list[str], config_file: str) -> list[str]:
    print (f'Verify tasks: {in_tasks}')

    error_list = []

    # parse file name to get the round_id
    round_id = Path(src_file).stem.split('-')[0]
    conf_pop_groups, conf_end_dates, conf_horizons = read_hub_config(config_file = config_file, round_id = round_id)

    # read the data frame from file 
    df = pd.read_parquet(src_file)
    
    # get unique values for the taskid
    if 'target_end_date' in in_tasks:
        in_ted = set([date_obj.strftime('%Y-%m-%d') for date_obj in df['target_end_date'].unique()])                
        
        if not in_ted.issubset(conf_end_dates):
            error_list.append(f'Invalid target_end_date: {in_ted - set(conf_end_dates)}')

    if 'pop_group' in in_tasks:
        in_pg = set(df['pop_group'].unique())
        
        if not in_pg.issubset(conf_pop_groups):
            error_list.append(f'Invalid pop_group_list: {in_pg - set(conf_pop_groups)}')


    # Define the N1 fields (grouping fields)
    group_fields = ['round_id', 'scenario_id', 'target', 'location', 'output_type_id']

    # Define the validation rules for N2 fields
    validation_rules = {
        'pop_group': conf_pop_groups,
        'horizon': conf_horizons,
    }


    # Run the validation
    results = validate_parquet_file(all_data = df, group_fields = group_fields, validation_rules = validation_rules)
    
    for group, result in results.items():
        if result is not True:
            # print(f"Group {group}: Missing combinations - {result}")
            # print(f"Missing combinations for group {group}")
            error_list.append('Missing some combinations')


    return error_list




if __name__ == "__main__":

    # input_list = []

    # input_file = str(args.input)

    # if input_file:
    #     input_list.append(input_file)
    # else:
    input_list = os.getenv("input_list").split(' ')
    

    tasks_list = args.taskids.split()
    config_file = str(args.configfile)

    for input_elem in input_list:
        print(f'Validating input: {input_elem}')
        if input_elem.startswith('model-output'):
            print('Validating model-output file')
            errors = check_task_ids(src_file = input_elem, in_tasks = tasks_list, config_file = config_file)
            if errors:
                print(f'Errors found, validation failed. Details: {errors}')
                exit (1)


    print ('Validation completed successfully!')
    exit(0)
