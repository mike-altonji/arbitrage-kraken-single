"""
Returns the cumulative reward up to this point in time.
In InfluxQL, we can look at the delta between entries with:
```
USE arbitrage;
SELECT difference("winnings_first") as winnings_first, difference("winnings_max") as winnings_max
FROM winnings
GROUP BY available_cash, min_roi
```
"""

from dotenv import load_dotenv
from influxdb import DataFrameClient
import os
import pandas as pd
import requests
import time

# InfluxDB Configs
start_time = time.time()
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
INPUT_TABLE= "arbitrage_details"
OUTPUT_TABLE = "winnings"

# Implementation configurations
AVAILABLE_CASH_LIST = [1_000, 2_500, 5_000, 10_000, 1_000_000]
MIN_ROI_LIST = [0, 0.0025]
MIN_TIME_SINCE_LAST_ARBITRAGE = "1H"

# Connect to InfluxDB
try:
    client = DataFrameClient("localhost", 8086, DB_USER, DB_PASSWORD, DB_NAME)
    client.ping()
    print("Client successfully created and connected to the server.")
except Exception as e:
    print("Failed to create client or connect to the server.")
    print("Error: ", e)

# Pull data
query = f"""
SELECT path, limiting_volume, ending_volume, volume_units
FROM {INPUT_TABLE}
WHERE volume_units = 'USD' OR volume_units = 'EUR'
"""
try:
    df = client.query(query=query)
    df = df[INPUT_TABLE]
    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"The fetched data is not a DataFrame: {type(df)}")
except Exception as e:
    print("Failed to fetch data from the server.")
    print("Error: ", e)

# Compute winnings
df["winnings"] = df["ending_volume"] - df["limiting_volume"]
df["roi"] = df["ending_volume"] / df["limiting_volume"] - 1

def limit_winnings_by_available_cash(row, limiter):
    if row["limiting_volume"] > limiter:
        row["limiting_volume"] = limiter
        row["ending_volume"] = (row["roi"] + 1) * row["limiting_volume"]
        row["winnings"] = row["ending_volume"] - row["limiting_volume"]
    return row

def calculate_time_diff(df):
    df = df.sort_index()  # Ensure the data is sorted by time
    df['time_diff'] = df.index.to_series().diff()  # Calculate the time difference between rows
    df['group'] = (df['time_diff'] > pd.Timedelta(MIN_TIME_SINCE_LAST_ARBITRAGE)).cumsum()
    return df

def convert_to_usd(df):
    non_usd_rows = df[df['volume_units'] != 'USD']
    for volume_unit in non_usd_rows['volume_units'].unique():
        try:
            response = requests.get(f'https://api.exchangerate-api.com/v4/latest/{volume_unit}')
            data = response.json()
            conversion_rate = data['rates']['USD']
            df.loc[df['volume_units'] == volume_unit, 'limiting_volume'] *= conversion_rate
            df.loc[df['volume_units'] == volume_unit, 'ending_volume'] *= conversion_rate
        except Exception as e:
            print(f"Failed to fetch conversion rate for {volume_unit}/USD.")
            print("Error: ", e)
    return df

# Convert all units to USD.
# NOTE: If there's a large swing in conversion, the diff in InfluxQL will swing as well.
df = convert_to_usd(df)

for min_roi in MIN_ROI_LIST:
    for available_cash in AVAILABLE_CASH_LIST:
        tmp = df.copy()

        # Group arbitrage rows of the same path until there's a break of `MIN_TIME_SINCE_LAST_ARBITRAGE` time between instances
        grouped = tmp.groupby('path').apply(calculate_time_diff).reset_index(drop=True)
        
        # Limit based on available cash, and filter out low ROI opportunities
        df_limited = grouped.apply(lambda row: limit_winnings_by_available_cash(row, available_cash), axis=1)
        df_high_roi = df_limited[df_limited["roi"] > min_roi]        

        # Grab the first, max winnings per opportunity group
        df_first = df_high_roi.groupby(['group', 'path']).first()['winnings']
        df_max = df_high_roi.groupby(['group', 'path']).max()['winnings']

        # Write these results to a new influxdb measurement
        point = pd.DataFrame(index=pd.DatetimeIndex([pd.Timestamp(start_time, unit='s')]),
                                    data={
                                        "available_cash": [available_cash],
                                        "min_roi": [min_roi],
                                        "winnings_first": [df_first.sum()], 
                                        "winnings_max": [df_max.sum()]
                                    })
        client.write_points(point, OUTPUT_TABLE, tag_columns=["available_cash", "min_roi"])

client.close()
