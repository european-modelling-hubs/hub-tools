
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

# Hardcoded: Optional rules per target
OPTIONAL_RULES = {
    'administered_doses': {
        'horizon': 'negative'  # values < 0 are optional
    }
}

def validate_parquet_file(all_data, group_fields, validation_rules, target_specific_rules=None):
    validation_fields = list(validation_rules.keys())
    validation_results = {}
    warnings = []

    grouped = all_data.groupby(group_fields)

    for group_values, group_df in grouped:
        existing_combinations = set(tuple(x) for x in group_df[validation_fields].drop_duplicates().values)
        expected_combinations = set(product(*[validation_rules[field] for field in validation_fields]))
        missing_combinations = expected_combinations - existing_combinations

        if missing_combinations:
            group_target = group_values[2]  # Assuming 'target' is the 3rd element

            if (
                target_specific_rules and
                group_target in target_specific_rules and
                'horizon' in target_specific_rules[group_target]
            ):
                rule = target_specific_rules[group_target]['horizon']
                optional, required = [], []

                for pg, h in missing_combinations:
                    try:
                        h_int = int(h)
                        if rule == 'negative' and h_int < 0:
                            optional.append((pg, h))
                        else:
                            required.append((pg, h))
                    except Exception:
                        required.append((pg, h))

                if required:
                    validation_results[group_values] = required
                if optional:
                    warnings.append(f"Group {group_values} (target={group_target}): Optional missing combinations - {optional}")
            else:
                validation_results[group_values] = list(missing_combinations)

    return validation_results, warnings

def read_hub_config(config_file: str, round_id: str):
    pop_groups, horizons, target_end_dates = [], [], []

    with open(config_file, 'r') as jconfig:
        print(f'Reading config file [{config_file}]...')
        jdata = json.load(jconfig)
        print('Loop over rounds...')

        for round in jdata['rounds']:
            r_id_var = round['round_id']
            task_ids = round['model_tasks'][0]['task_ids']
            r_id = task_ids[r_id_var]['required'][0]

            if round_id == r_id:
                pop_groups = task_ids['pop_group']['optional']
                target_end_dates = task_ids['target_end_date']['optional']
                horizons = task_ids['horizon']['optional']
            else:
                continue

    return (pop_groups, target_end_dates, horizons)

def check_task_ids(src_file: str, in_tasks: list[str], config_file: str) -> list[str]:
    print(f'Verify tasks: {in_tasks}')
    error_list = []

    round_id = Path(src_file).stem.split('-')[0]
    conf_pop_groups, conf_end_dates, conf_horizons = read_hub_config(config_file, round_id)

    df = pd.read_parquet(src_file)

    if 'target_end_date' in in_tasks:
        in_ted = set([date_obj.strftime('%Y-%m-%d') for date_obj in df['target_end_date'].unique()])
        if not in_ted.issubset(conf_end_dates):
            error_list.append(f'Invalid target_end_date: {in_ted - set(conf_end_dates)}')

    if 'pop_group' in in_tasks:
        in_pg = set(df['pop_group'].unique())
        if not in_pg.issubset(conf_pop_groups):
            error_list.append(f'Invalid pop_group_list: {in_pg - set(conf_pop_groups)}')

    group_fields = ['round_id', 'scenario_id', 'target', 'location', 'output_type_id']
    validation_rules = {
        'pop_group': conf_pop_groups,
        'horizon': conf_horizons,
    }

    results, warnings = validate_parquet_file(
        all_data=df,
        group_fields=group_fields,
        validation_rules=validation_rules,
        target_specific_rules=OPTIONAL_RULES
    )

    for group, result in results.items():
        if result is not True:
            error_list.append(f"Group {group}: Missing combinations - {result}")

    if warnings:
        print("⚠️  Warnings (non-blocking):")
        for w in warnings:
            print(" -", w)

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
