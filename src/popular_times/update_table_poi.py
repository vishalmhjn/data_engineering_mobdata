import psycopg2
from popular_times.create_tables import db_params
from io import StringIO
import sys


def update_table_wrapper(df):
    # Define the table name
    table_name = "pois_munich"

    # Create a buffer to hold the DataFrame as CSV data
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    # Establish a database connection
    connection = psycopg2.connect(**db_params)

    # Create a cursor object
    cursor = connection.cursor()

    # Check if a similar record exists
    check_query = f"SELECT * FROM {table_name} WHERE datetime = %s"
    cursor.execute(check_query, (df.datetime[0],))
    existing_record = cursor.fetchone()
    print(df.datetime[0])

    if existing_record is None:
        # Insert the data if no similar record exists

        try:
            # Use the PostgreSQL COPY command to copy data from the buffer to the table
            copy_sql = f"COPY {table_name} FROM stdin WITH CSV HEADER DELIMITER as ','"
            cursor.copy_expert(sql=copy_sql, file=csv_buffer)

            # Commit the transaction
            connection.commit()

            print("Data from CSV file loaded into the table successfully!")

        except psycopg2.Error as error:
            print("Error: ", error)

        finally:
            if connection:
                cursor.close()
                connection.close()
    else:
        # Handle the duplicate record as needed (e.g., update or skip)
        print("Duplicate record found. You can choose to update or skip.")
        cursor.close()
        connection.close()
