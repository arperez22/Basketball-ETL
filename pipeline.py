from extract import extract_data
from transform import transform_data
from load import load_data


def run_pipeline(conn):
    extract_data()
    transform_data()
    load_data(conn)