import psycopg2


def get_server_pass():
    SERVER_PASS_FILE = (
        "/home/vishal/Documents/git/dags/src/popular_times/input/SERVER_PASS"
    )
    SERVER_PASS = open(SERVER_PASS_FILE, "r").readline().rstrip()
    return SERVER_PASS


# Database connection parameters
db_params = {
    "host": "localhost",
    "database": "popular_times",
    "user": "postgres",
    "password": get_server_pass(),
    "port": 5432,
}


if __name__ == "__main__":
    # Define column names and their data types
    column_data_types = {
        "datetime": "timestamp",
        "name": "varchar(255)",
        "city": "varchar(255)",
        "types": "varchar(255)",
        "rating": "float",
        "rating_n": "integer",
        "curr_pop": "integer",
        "time_spent_min": "integer",
        "time_spent_max": "integer",
    }

    table_name = "pois_munich"
    # Generate column definitions for the time slots (Mon-Sun, 0-23)
    time_slots = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    for attr in ["pp"]:
        for day in time_slots:
            for hour in range(24):
                column_data_types[f"{attr}_{day}_{hour}"] = "integer"

    # Create the SQL statement for creating the table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {', '.join([f'{col} {data_type}' for col, data_type in column_data_types.items()])}
    );
    """

    try:
        # Connect to the PostgreSQL database
        connection = psycopg2.connect(**db_params)

        # Create a cursor object
        cursor = connection.cursor()

        # Execute the CREATE TABLE SQL statement
        cursor.execute(create_table_sql)

        # Commit the transaction
        connection.commit()

        # Close the cursor and connection
        cursor.close()
        connection.close()

        print(f"Table {table_name} created successfully!")

    except psycopg2.Error as error:
        print("Error: ", error)

    finally:
        if connection:
            connection.close()
