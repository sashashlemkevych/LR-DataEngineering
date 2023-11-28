import glob
import json
import csv
from flatten_json import flatten


def process_json_file(json_path):
    """
    Process a single JSON file and convert it to a flattened CSV file.
    """
    with open(json_path, "r") as json_file:
        my_json = json.load(json_file)

        flattened_json = []
        if isinstance(my_json, list):
            for item in my_json:
                if isinstance(item, dict):
                    flattened_json.append(flatten(item))
        elif isinstance(my_json, dict):
            flattened_json = [flatten(my_json)]

        return flattened_json


def save_to_csv(csv_path, data):
    """
    Save the flattened JSON data to a CSV file.
    """
    with open(csv_path, 'w', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)

        header = data[0].keys() if data else []
        csv_writer.writerow(header)

        for item in data:
            csv_writer.writerow(item.values())


def main():
    root_directory = 'data'

    json_paths = [file for file in glob.glob(
        root_directory + '/**/*.json', recursive=True)]

    for json_path in json_paths:
        flattened_json = process_json_file(json_path)

        # Adjusting the CSV file path
        csv_path = json_path[:-4] + 'csv'

        # Save flattened JSON data to CSV
        save_to_csv(csv_path, flattened_json)


if __name__ == "__main__":
    main()
