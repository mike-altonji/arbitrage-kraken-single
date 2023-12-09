"""
Returns the reward per hour for the prior 3 days.
USD and EUR are considered separately, avoiding unit conversions.
In Influx, will OVERWRITE the prior 3 days of values per hour, by design. 
Allows for overwriting `max` values for arbitrage opportunities spanning multiple hours.
In InfluxQL, can create a cumulative chart as well.

Need both clients because:
- DataFrameClient doesn't allow us to properly infer the format of our timestamps
- InfluxDBClient requires a lot of boilerplate to write points from a df
"""

from dotenv import load_dotenv
from influxdb import InfluxDBClient, DataFrameClient
import os
import pandas as pd

# InfluxDB Configs
load_dotenv()
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
INPUT_TABLE= "arbitrage_details"
OUTPUT_TABLE = "winnings"

# Implementation configurations
AVAILABLE_CASH_LIST = [1_000, 2_500, 5_000, 10_000, 1_000_000]
MIN_ROI_LIST = [0, 0.0025]
MIN_VOLUME_LIST = [0, 10]
STARTER_LIST = ["USD", "EUR"]
MIN_TIME_SINCE_LAST_ARBITRAGE = "1H"

# Connect to InfluxDB
try:
    client = InfluxDBClient("localhost", 8086, DB_USER, DB_PASSWORD, DB_NAME)
    client_df = DataFrameClient("localhost", 8086, DB_USER, DB_PASSWORD, DB_NAME)
    client.ping()
    client_df.ping()
    print("Client successfully created and connected to the server.")
except Exception as e:
    print("Failed to create client or connect to the server.")
    print("Error: ", e)

# Pull data
query = f"""
SELECT path, limiting_volume, ending_volume, volume_units
FROM {INPUT_TABLE}
WHERE 
    time > now() - 3d 
    AND (volume_units = 'USD' OR volume_units = 'EUR')
"""
try:
    result = client.query(query=query)
    df = pd.DataFrame(result.get_points())
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
    df['time'] = pd.to_datetime(df['time'], format="ISO8601")
    df = df.sort_values(by=["time"])  # Ensure the data is sorted by time
    df['time_diff'] = df["time"].diff()  # Calculate the time difference between rows
    df['group'] = (df['time_diff'] > pd.Timedelta(MIN_TIME_SINCE_LAST_ARBITRAGE)).cumsum()
    return df

for starter in STARTER_LIST:
    df2 = df[df["volume_units"] == starter]
    for min_vol in MIN_VOLUME_LIST:
        for min_roi in MIN_ROI_LIST:
            for available_cash in AVAILABLE_CASH_LIST:
                tmp = df2.copy()

                # Group arbitrage rows of the same path until there's a break of `MIN_TIME_SINCE_LAST_ARBITRAGE` time between instances
                grouped = tmp.groupby('path').apply(calculate_time_diff).reset_index(drop=True)

                # Limit based on available cash, and filter out low ROI opportunities
                df_limited = grouped.apply(lambda row: limit_winnings_by_available_cash(row, available_cash), axis=1)
                df_high_roi = df_limited[df_limited["roi"] > min_roi]
                df_high_roi = df_high_roi[df_high_roi["limiting_volume"] > min_vol]  # Do after calculating time diff

                # Grab the first, max winnings per opportunity group
                df_first = df_high_roi.groupby(['group', 'path']).first()[['winnings', 'time']]
                idx_max = df_high_roi.groupby(['group', 'path'])['winnings'].idxmax()
                df_max = df_high_roi.loc[idx_max, ['winnings', 'time']]

                # Resample the data to hourly intervals and sum the winnings
                df_first.set_index('time', inplace=True)
                df_max.set_index('time', inplace=True)
                df_first_hourly = df_first.resample('H').sum()
                df_max_hourly = df_max.resample('H').sum()

                # Create a new DataFrame with 'first' and 'max' columns
                df_hourly_winnings = pd.DataFrame({
                    'first': df_first_hourly['winnings'],
                    'max': df_max_hourly['winnings']
                })
                df_hourly_winnings["starter"] = starter
                df_hourly_winnings["min_volume"] = min_vol
                df_hourly_winnings["available_cash"] = available_cash
                df_hourly_winnings["min_roi"] = min_roi

                # Write these results to InfluxDB
                client_df.write_points(df_hourly_winnings, OUTPUT_TABLE, tag_columns=["starter", "min_volume", "available_cash", "min_roi"])

client.close()
client_df.close()
