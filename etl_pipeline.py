import pandas as pd
import os

input_file = "input/data.csv"
output_file = "output/processed_data.csv"

def extract(file_path):
    """Extract the data from csv"""     
    data = pd.read_csv(file_path)
    return data

def transform(data):
    """Transform data by filter"""
    filtered_data = data[data["age"] > 25]
    filtered_data['age_in_5_years'] = filtered_data['age'] + 5
    return filtered_data

def load(data, output_path):
    """Load data as new  csv file"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    data.to_csv(output_path, index=False)
    print(f"Data is successfully written to {output_path} location")


def main():
    """Main ETL Process"""
    data = extract(input_file)
    transformed_data = transform(data)
    load(transformed_data, output_file)
    print("ETL pipeline completed")

if __name__ == "__main__":
    main()