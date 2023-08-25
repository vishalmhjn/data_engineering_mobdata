import psycopg2
from io import StringIO
import sys
import pandas as pd
import logging
from popular_times.create_tables import db_params
from popular_times.process_scrapped_poi import wrapper_process_scrapped


def update_table_wrapper(df):
    # Define the table name
    table_name = "pois_munich"

    # Create a buffer to hold the DataFrame as CSV data
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    try:
        # Establish a database connection
        with psycopg2.connect(**db_params) as connection:
            # Create a cursor object
            with connection.cursor() as cursor:
                # Check if a similar record exists
                check_query = f"SELECT * FROM {table_name} WHERE datetime = %s"
                cursor.execute(check_query, (df.datetime[0],))
                existing_record = cursor.fetchone()
                logging.info(df.datetime[0])

                if existing_record is None:
                    # Insert the data if no similar record exists
                    copy_sql = (
                        f"COPY {table_name} FROM stdin WITH CSV HEADER DELIMITER as ','"
                    )
                    cursor.copy_expert(sql=copy_sql, file=csv_buffer)

                    # Commit the transaction
                    connection.commit()

                    print("Data from CSV file loaded into the table successfully!")

                else:
                    # Handle the duplicate record as needed (e.g., update or skip)
                    print("Duplicate record found. You can choose to update or skip.")

    except psycopg2.Error as error:
        # Handle database errors
        logging.error(f"Error: {error}")


if __name__ == "__main__":
    input_file_path = "data/poipopulartimes_20230825-142252.csv"
    df = pd.read_csv(input_file_path)
    processed_df = wrapper_process_scrapped(df, input_file_path)

    try:
        update_table_wrapper(processed_df)
    except Exception as e:
        logging.error(f"An error occurred while updating the table: {e}")

    print("Done")
