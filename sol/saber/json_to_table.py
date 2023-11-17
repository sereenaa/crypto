import pandas as pd
import json

with open('saber.json', 'r') as file:
    data = json.load(file)


def flatten_json(json_obj, sep='_'):
    def flatten(item, prefix='', sep=sep):
        result = {}
        if isinstance(item, dict):
            for key, value in item.items():
                new_key = prefix + sep + key if prefix else key
                if isinstance(value, dict):
                    result.update(flatten(value, new_key, sep=sep))
                elif isinstance(value, list):
                    for i, elem in enumerate(value):
                        if isinstance(elem, dict):
                            result.update(flatten(elem, f'{new_key}{sep}{i}', sep=sep))
                        else:
                            result[f'{new_key}{sep}{i}'] = elem
                else:
                    result[new_key] = value
        elif isinstance(item, list):
            for i, elem in enumerate(item):
                result.update(flatten(elem, f'{prefix}{sep}{i}', sep=sep))
        return result

    if isinstance(json_obj, list):
        return {f'row{index}': flatten(item) for index, item in enumerate(json_obj)}
    else:
        return {key: flatten(value) for key, value in json_obj.items()}

def json_to_table(file_path):
    with open(file_path, 'r') as f:
        json_obj = json.load(f)
    flattened = flatten_json(json_obj)
    df = pd.DataFrame(flattened).T  # Transpose to get keys as rows
    return df


df = json_to_table('saber.json')
df.to_csv('pools.csv')