import datetime
import pandas as pd
import numpy as np

DAYS_OF_WEEK = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
HOURS_OF_DAY = list(range(24))


def wrapper_process_scrapped(df, file_name):
    """
    Process scrapped data and return a cleaned DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.
        file_name (str): Name of the input file.

    Returns:
        pd.DataFrame: Processed DataFrame.
    """
    df.dropna(subset=["popular_times"], inplace=True)
    df = df[df["popular_times"] != "[]"]
    df.reset_index(inplace=True, drop=True)

    date_str = file_name.split("_")[2][:-4]
    date = datetime.datetime.strptime(date_str, "%Y%m%d-%H%M%S")

    df["rating"] = df["rating"].fillna(-1)
    df["rating_n"] = df["rating_n"].fillna(-1)

    df["current_popularity"] = df["current_popularity"].fillna(-1)
    current_popularity = df["current_popularity"].astype(int)

    # Replace NaN values with empty lists using a lambda function
    df["time_spent"] = df["time_spent"].apply(
        lambda x: [-1, -1] if pd.isna(x) else eval(x)
    )

    # Extract elements from the lists and create two new columns
    df["time_spent_min"] = df["time_spent"].apply(
        lambda x: x[0] if len(x) >= 1 else np.nan
    )
    df["time_spent_max"] = df["time_spent"].apply(
        lambda x: x[1] if len(x) >= 2 else np.nan
    )

    rating = df["rating"].astype(float)
    rating_n = df["rating_n"].astype(int)

    pp_times = []

    for i, popular_times_str in enumerate(df["popular_times"]):
        popular_times = eval(popular_times_str)
        daily_popularity = []

        for day in DAYS_OF_WEEK:
            hourly_data = popular_times[DAYS_OF_WEEK.index(day)]["data"]
            daily_popularity.extend(hourly_data)

        if i == 0:
            pp_times = daily_popularity
        else:
            pp_times = np.vstack((pp_times, daily_popularity))

    df_new = pd.DataFrame(pp_times)
    df_new = df_new.astype(int)
    df_new["datetime"] = date
    df_new["name"] = df["name"]
    df_new["city"] = df["address"]
    df_new["types"] = df["node_type"]
    df_new["rating"] = rating
    df_new["rating_n"] = rating_n
    df_new["curr_pop"] = current_popularity
    df_new["time_spent_min"] = df["time_spent_min"]
    df_new["time_spent_max"] = df["time_spent_max"]

    # Create column names for the hourly popularity data
    col_names = []

    for attr in ["pp"]:
        for day in DAYS_OF_WEEK:
            for hour in HOURS_OF_DAY:
                col_names.append(f"{attr}_{day}_{hour}")

    # Append other columns
    col_names.extend(
        [
            "datetime",
            "name",
            "city",
            "types",
            "rating",
            "rating_n",
            "curr_pop",
            "time_spent_min",
            "time_spent_max",
        ]
    )

    df_new.columns = col_names

    # Reorder columns
    cols = col_names[-9:] + col_names[:-9]
    df_new = df_new[cols]

    return df_new


if __name__ == "__main__":
    input_file_path = "data/poipopulartimes_20230825-142252.csv"
    df = pd.read_csv(input_file_path)
    processed_df = wrapper_process_scrapped(df, input_file_path)
    processed_df.to_csv(
        "processed_data.csv", index=False
    )  # Optionally, save the processed data to a CSV file
    print("Done")
