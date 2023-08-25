import pandas as pd
from popular_times.process_scrapped_poi import wrapper_process_scrapped
from popular_times.get_popular_times_poi import wrapper_download_raw
from popular_times.update_table_poi import update_table_wrapper


def main():
    filename = wrapper_download_raw()
    df = pd.read_csv(filename)
    df = wrapper_process_scrapped(df, filename)
    update_table_wrapper(df)


if __name__ == "__main__":
    # main()
    from glob import glob

    files = sorted(glob("data/poi*"))
    for filename in files:
        df = pd.read_csv(filename)
        df = wrapper_process_scrapped(df, filename)
        update_table_wrapper(df)
