# Code to load and preprocess the CSV file.
import pandas as pd

def load_data(file_path):
    return pd.read_csv(file_path)

