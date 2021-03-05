"""
This script tests the original solution and the new solution and returns the time difference between both.
"""

from file_manager import FileManager
from feature_extractor import FeatureExtractor
import pandas as pd
import time

###
# New Pipeline
###
print("Start new Pipeline")
initial_time_new_solution = time.time()

file_manager = FileManager("../data/rating.parquet.gzip")
df = file_manager.get_dataframe()
df = df.loc[0:10000, :]
feature_extractor = FeatureExtractor(df)
feature_extractor.extract_nb_previous_ratings()
feature_extractor.extract_avg_ratings_previous()

feature_extractor.save_dataframe("./test_outputs/all_features.parquet.gzip")

final_time_new_solution = time.time() - initial_time_new_solution
print(f"Finished in {final_time_new_solution}s")


###
# Original Pipeline
###
print("Start original Pipeline")
initial_time_original_solution = time.time()
df_ratings = pd.read_csv("../data/rating.csv")
df_ratings = df_ratings.loc[0:10000, :]
df_ratings = df_ratings.sort_values(by=["userId", "timestamp"])
df_ratings = df_ratings.reset_index(drop=True)
df_ratings["timestamp"] = pd.to_datetime(df_ratings.timestamp)
df_grouped = df_ratings.groupby("userId")
df_ratings["nb_previous_ratings"] = df_grouped["timestamp"].rank(method="first") - 1


def avg_previous(df_or):
    avg = pd.Series(index=df_or.index, dtype="float64")
    for i in df_or.index:
        df_aux = df_or.loc[df_or.timestamp < df_or.timestamp.loc[i], :]
        avg.at[i] = df_aux.rating.mean()
    return avg


avg_ratings_previous = pd.Series(dtype="float64")
# the following cycle is the one that takes forever if we try to compute it for the whole dataset
for user in df_ratings.userId.unique():
    df_user = df_ratings.loc[df_ratings.userId == user, :]
    avg_ratings_previous = avg_ratings_previous.append(avg_previous(df_user))
df_ratings["avg_ratings_previous"] = avg_ratings_previous

df_ratings.to_csv("./test_outputs/all_features_original_pipeline.csv")

final_time_original_solution = time.time() - initial_time_original_solution
print(f"Finished in {final_time_original_solution}s")

print(f"The new solution is {final_time_original_solution - final_time_new_solution} faster than the original")
