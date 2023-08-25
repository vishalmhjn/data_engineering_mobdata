import pandas as pd
import logging
from popular_times.process_scrapped_poi import wrapper_process_scrapped
from popular_times.get_popular_times_poi import wrapper_download_raw
from popular_times.update_table_poi import update_table_wrapper


def main():
    try:
        # Step 1: Download raw data
        filename = wrapper_download_raw()

        # Step 2: Process the raw data
        df = pd.read_csv(filename)
        df = wrapper_process_scrapped(df, filename)

        # Step 3: Update the database table
        update_table_wrapper(df)

        # Log success
        logging.info("Data processing completed successfully.")

    except Exception as e:
        # Handle exceptions and log errors
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(filename="app.log", level=logging.INFO)

    # Execute the main function
    # main()

    # from glob import glob

    # files = sorted(glob("data/poi*"))
    # for filename in files:
    #     df = pd.read_csv(filename)
    #     df = wrapper_process_scrapped(df, filename)
    #     update_table_wrapper(df)
