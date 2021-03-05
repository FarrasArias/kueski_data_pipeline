import pandas as pd
import numpy as np


# Class created to manage files, turn them into DataFrames and add columns to them.
# It can also save a DataFrame in gzip, csv and json files.
class FileManager:
    def __init__(self, file_path):
        self.file_path = file_path
        self.extension = file_path[-10:].split(".")[-1]
        self.dataframe = None

    def get_file_path(self):
        return self.file_path

    def get_dataframe(self):
        if self.dataframe is None:
            self.read_file(self.file_path)
        return self.dataframe

    def get_specific_columns_from_dataframe(self, list_columns):
        if self.dataframe is None:
            self.dataframe = self.get_dataframe()

        if np.max(list_columns) < len(self.dataframe.columns):
            return self.dataframe.iloc[:, list_columns]
        else:
            raise Exception("List contains column indexes that are not valid.")

    def read_file(self, path):
        if self.extension == "gzip":
            df = pd.read_parquet(path)
        elif self.extension == "csv":
            df = pd.read_csv(path)
        elif self.extension == "json":
            df = pd.read_json(path)
        else:
            raise Exception("Format not implemented.")
        self.dataframe = df

    def add_column_to_dataframe(self, column_name, series):
        assert isinstance(series, pd.Series), "arg 2 must be a pandas Series"
        assert series.shape[0] == self.dataframe.shape[0], "arg 2 must be the same length as dataframe rows"
        self.dataframe[column_name] = series

    def save_dataframe(self, path, **kwargs):
        if "extension" in kwargs.keys():
            ext = kwargs["extension"]
        else:
            ext = self.extension

        if ext == "gzip":
            self.dataframe.to_parquet(path+".parquet.gzip", compression="gzip")
        elif ext == "csv":
            self.dataframe.to_csv(path+".csv")
        elif ext == "json":
            self.dataframe.to_json(path+".json")
        else:
            raise Exception("Format not implemented.")
