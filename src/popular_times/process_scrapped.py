import datetime
import pandas as pd
import numpy as np

import sys


def wrapper_process_scrapped(df, file_name):
    df.dropna(subset=["popular_times"], inplace=True)
    df.reset_index(inplace=True, drop=True)

    date = file_name.split("_")[2][:-4]
    date = str(
        datetime.datetime(
            int(date[:4]),
            int(date[4:6]),
            int(date[6:8]),
            int(date[9:11]),
            int(date[11:13]),
            00,
        )
    )
    address = df["city"]
    types = df["address"]
    name = df["name"]
    df["rating"] = df["rating"].fillna(-1)
    df["rating_n"] = df["rating_n"].fillna(-1)
    df["current_popularity"] = df["current_popularity"].fillna(-1)
    rating = df["rating"].astype(float)
    rating_n = df["rating_n"].astype(int)
    popular_times = df["popular_times"]
    current_popularity = df["current_popularity"].astype(int)

    for i in range(0, len(popular_times)):
        a = []
        for day in range(0, 7):
            a.extend(eval(popular_times[i])[day]["data"])
        if i == 0:
            pp_times = a
        else:
            pp_times = np.vstack((pp_times, a))
    df_new = pd.DataFrame(pp_times)
    df_new = df_new.astype(int)
    df_new["datetime"] = date
    df_new["name"] = name
    df_new["city"] = address
    df_new["type"] = types
    df_new["rating"] = rating
    df_new["rating_n"] = rating_n
    df_new["curr_pop"] = current_popularity
    col_names = []

    for idfr in ["pp"]:
        for day in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
            for hour in range(0, 24):
                col_names.append(idfr + "_" + day + "_" + str(hour))
    col_names.extend(
        [
            "datetime",
            "name",
            "city",
            "types",
            "rating",
            "rating_n",
            "curr_pop",
        ]
    )
    df_new.columns = col_names
    cols = col_names[-7:] + col_names[:-7]
    df_new = df_new[cols]
    return df_new


if __name__ == "__main__":
    input_file_path = sys.argv[1]
    df = pd.read_csv(input_file_path)
    wrapper_process_scrapped(df, input_file_path)
    print("Done")
