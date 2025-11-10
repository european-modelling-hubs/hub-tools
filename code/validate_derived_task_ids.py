
import os
import json
import argparse
import pandas as pd
from itertools import product
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--configfile', default='hub-config/tasks.json')
parser.add_argument('-t', '--taskids', default='target_end_date pop_group')
args = parser.parse_args()

def validate_parquet_file(all_data, group_fields, validation_rules):
    validation_fields = list(validation_rules.keys())
    validation_results = {}
    grouped = all_data.groupby(group_fields)
    for group_values, group_df in grouped:
        existing_combinations = set(tuple(x) for x in group_df[validation_fields].drop_duplicates().values)
        expected_combinations = set(product(*[validation_rules[field] for field in validation_fields]))
        missing_combinations = expected_combinations - existing_combinations
        if missing_combinations:
            validation_results[group_values] = list(missing_combinations)
        else:
            validation_results[group_values] = True
    return validation_results

def read_hub_config(config_file: str, round_id: str, target: str) -> tuple[list[str], list[str], list[int]]:
    pop_groups = []
    horizons = []
    target_end_dates = []
    with open(config_file, 'r') as jconfig:
        print(f'Reading config file [{config_file}]...')
        jdata = json.load(jconfig)
        print('Loop over rounds...')
        for round in jdata['rounds']:
            r_id_var = round['round_id']
            model_tasks = round['model_tasks']
            for task in model_tasks:
                task_ids = task['task_ids']
                r_id = task_ids[r_id_var]['required'][0]
                targets = task_ids['target']['required']
                if round_id == r_id and target in targets:
                    pop_groups = task_ids['pop_group']['optional']
                    target_end_dates = task_ids['target_end_date']['optional']
                    horizons = task_ids['horizon']['optional']
                    return (pop_groups, target_end_dates, horizons)
    raise ValueError(f"No matching config found for round_id={round_id}, target={target}")

def check_task_ids(src_file: str, in_tasks: list[str], config_file: str) -> list[str]:
    print(f'Verify tasks: {in_tasks}')
    error_list = []

    round_id = Path(src_file).stem.split('-')[0]
    df = pd.read_parquet(src_file)
    
    # Cicla su ciascun target presente nel file
    for target in df['target'].unique():
        print(f"→ Checking target: {target}")
        df_target = df[df['target'] == target].copy()
        
        # Carica la config appropriata per questo target
        try:
            conf_pop_groups, conf_end_dates, conf_horizons = read_hub_config(
                config_file=config_file,
                round_id=round_id,
                target=target
            )
        except ValueError as e:
            error_list.append(str(e))
            continue

        # 1️⃣ CHeck target_end_date
        if 'target_end_date' in in_tasks and 'target_end_date' in df_target.columns:
            in_ted = set([date_obj.strftime('%Y-%m-%d') for date_obj in df_target['target_end_date'].unique()])
            if not in_ted.issubset(conf_end_dates):
                error_list.append(f'Invalid target_end_date: {in_ted - set(conf_end_dates)} (target={target})')

        # 2️⃣ Check pop_group based on target
        if 'pop_group' in in_tasks and 'pop_group' in df_target.columns:
            in_pg = set(df_target['pop_group'].unique())
            if not in_pg.issubset(conf_pop_groups):
                error_list.append(f'Invalid pop_group_list: {in_pg - set(conf_pop_groups)} for target "{target}"')

        # 3️⃣ Validate combinations (if needed)
        group_fields = ['round_id', 'scenario_id', 'target', 'location', 'output_type_id']
        validation_rules = {
            'pop_group': conf_pop_groups,
            'horizon': conf_horizons,
        }

        results = validate_parquet_file(
            all_data=df_target,
            group_fields=group_fields,
            validation_rules=validation_rules
        )

        for group, result in results.items():
            if result is not True:
                error_list.append(f"Group {group} (target={target}): Missing combinations - {result}")

    return error_list

if __name__ == "__main__":
    input_list = os.getenv("input_list").split(' ')
    tasks_list = args.taskids.split()
    config_file = str(args.configfile)
    for input_elem in input_list:
        print(f'Validating input: {input_elem}')
        if input_elem.startswith('model-output'):
            print('Validating model-output file')
            errors = check_task_ids(src_file=input_elem, in_tasks=tasks_list, config_file=config_file)
            if errors:
                print(f'Errors found, validation failed. Details: {errors}')
                exit(1)
    print('Validation completed successfully!')
    exit(0)
