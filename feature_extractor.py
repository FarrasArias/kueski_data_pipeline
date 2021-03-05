import pandas as pd
import numpy as np

""" 
Class created to extract features from a given dataframe.
- The class can extract features individually.
- It can add them to the original dataframe or return the features as a Pandas Series.
- Finally it can save the DataFrame with the added features, or save only the added features in a new DataFrame.
  To specify the file extension, simply write it on the path. E.G.: "./data/fileName.csv" would save it as a csv.
"""
class FeatureExtractor:
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.sort_and_convert()
        self.added_features = []

    def get_dataframe(self):
        return self.dataframe

    # Sort the DataFrame by userId and timestamp (in that order) and convert timestamp strings into pandas timestamps.
    def sort_and_convert(self):
        self.dataframe = self.dataframe.sort_values(by=["userId", "timestamp"])
        self.dataframe = self.dataframe.reset_index(drop=True)
        self.dataframe["timestamp"] = pd.to_datetime(self.dataframe.timestamp)

    # Extract the number of previous ratings of a user before the current one.
    def extract_nb_previous_ratings(self, add=True):
        column_name = "nb_previous_ratings"
        feature_series = self.dataframe.groupby("userId")["timestamp"].rank(method="first") - 1
        if add:
            self.dataframe[column_name] = feature_series
            self.added_features.append(column_name)
        else:
            return feature_series

    # Extract the average of the ratings scores of a user before the current one.
    def extract_avg_ratings_previous(self, add=True):
        column_name = "avg_ratings_previous"
        if "nb_previous_ratings" not in self.dataframe.columns:
            nb = self.extract_nb_previous_ratings(add=False)
        else:
            nb = self.dataframe["nb_previous_ratings"]
        rating_cumsum = self.dataframe.groupby("userId").rating.cumsum()
        avg_ratings_previous = rating_cumsum / (nb + 1)
        avg_ratings_previous = avg_ratings_previous.shift(1)
        avg_ratings_previous[nb == 0.0] = np.nan

        if add:
            self.dataframe[column_name] = avg_ratings_previous
            self.added_features.append(column_name)
        else:
            return avg_ratings_previous

    # Save the dataframe
    def save_dataframe(self, path, save_only_added_features=False):
        extension = path[-10:].split(".")[-1]

        if save_only_added_features:
            df = self.dataframe.loc[:, self.added_features]
        else:
            df = self.dataframe

        if extension == "gzip":
            df.to_parquet(path, compression="gzip")
        elif extension == "csv":
            df.to_csv(path)
        elif extension == "json":
            df.to_json(path)
        else:
            raise Exception("Format not implemented.")
