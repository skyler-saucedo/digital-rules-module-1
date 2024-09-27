
import json
import csv
import os
from copy import deepcopy
import pandas as pd
import numpy as np
from dotenv import load_dotenv

def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for right_row in right:
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows


def flatten_list(data):
    for elem in data:
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem


def json_to_dataframe(data_in):
    def flatten_json(data, prev_heading=''):
        if isinstance(data, dict):
            rows = [{}]
            for key, value in data.items():
                rows = cross_join(rows, flatten_json(value, prev_heading + '.' + key))
        elif isinstance(data, list):
            rows = []
            for item in data:
                [rows.append(elem) for elem in flatten_list(flatten_json(item, prev_heading))]
        else:
            rows = [{prev_heading[1:]: data}]
        return rows

    return pd.DataFrame(flatten_json(data_in))


def rename_column(column_name):
    r = column_name.split('.')
    result = []
    current_element = r[0]
    count = 1

    for element in r[1:]:
        if element == current_element:
            count += 1
        else:
            if count > 1:
                result.append(f"{current_element}_{count}")
            else:
                result.append(current_element)
            current_element = element
            count = 1

    # Append the last element
    if count > 1:
        result.append(f"{current_element}_{count}")
    else:
        result.append(current_element)

    # combine 
    new_col_name = ''
    for z in result:
        new_col_name = new_col_name + '-' + z
    new_col_name = new_col_name[1:]

    return new_col_name

def rename_columns(df):
    new_columns = {}
    for col in df.columns:
        new_columns[col] = rename_column(col)
    # Rename the columns
    df.rename(columns=new_columns, inplace=True)

    # add index name to header
    df.index.name = 'index'
    return df

def remove_empties(df):
    """
    use this to remove empty elements in df
    NaN already declared in dataframe, 
    but Neo4j does not like NaNs, so we will add the string version
    """
    
    return df.fillna('NAN')



def main():
    """
    convert lsu jsons to csv.
    """

    load_dotenv()  # Load environment variables from .env file
    json_directory = os.getenv('JSON_DIR')
    json_file = os.getenv('JSON_FILE')

    # Iterate over each JSON file in the directory
    for filename in os.listdir(json_directory):
        if filename.endswith('.json'):
            # Load the JSON data from the file
            print(f"loading file: {filename}")

            with open(filename) as jf:
                json_data = json.load(jf)

            df = json_to_dataframe(json_data)

            # lots of redundant column names, like children.children.children... let's shorten them...

            df = rename_columns(df)

            # remove empties
            df = remove_empties(df)

            # split name for csv
            fn = filename.split('.')

            csvfilename = fn[0] + '.csv'

            df.to_csv(csvfilename, mode='w')
            print(f"csv saved to: {csvfilename}")

if __name__ == '__main__':
    main()