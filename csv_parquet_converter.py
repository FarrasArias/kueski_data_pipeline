# Class created to convert .csv files to hdf5 files for faster processing.
import time
import pandas as pd


print("Converting the file...")

df_csv = pd.read_csv("./data/rating.csv")
df_csv.to_parquet("./data/rating.parquet.gzip", compression="gzip")

print("Calculating the time difference...")

time_x = time.time()
df_csv = pd.read_csv("./data/rating.csv")
time_csv = time.time() - time_x

time_y = time.time()
df_csv = pd.read_parquet("./data/rating.parquet.gzip")
time_parquet = time.time() - time_y

print(f"It takes {time_csv}s to read the original rating.csv.")
print(f"It takes {time_parquet}s to read the parquet file.")
print(f"In took {time_csv-time_parquet}s less to read the file")
