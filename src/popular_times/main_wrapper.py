import pandas as pd
from popular_times.update_table import update_table_wrapper
from popular_times.process_scrapped import wrapper_process_scrapped
from popular_times.get_popular_times import wrapper_download_raw


def main():
    filename = wrapper_download_raw()
    df = pd.read_csv(filename)
    df = wrapper_process_scrapped(df, filename)
    update_table_wrapper(df)
