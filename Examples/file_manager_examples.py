# This script contains some examples on how to use the FileManager class
from file_manager import FileManager
import numpy as np
import pandas as pd

# To read a file.
# (Note that an extension must not be specified. It is extracted from the path)
file_manager_1 = FileManager("../data/rating.parquet.gzip")
# The next line reads the file and generates a Pandas DataFrame.
file_manager_1.read_file(file_manager_1.get_file_path())
print("Example 1 finished. File Read.")

# To get a DataFrame from the file.
file_manager_2 = FileManager("../data/rating.parquet.gzip")
df_2 = file_manager_2.get_dataframe()
print("Example 2 finished.")
print(file_manager_2.get_dataframe().head(3))

# To get specific columns from the DataFrame.
# (Note that we don't have to call the previous methods. However, if the previous methods have been called,
# the method will use the extracted DataFrame to speed up the process)
file_manager_3 = FileManager("../data/rating.parquet.gzip")
df_3 = file_manager_3.get_specific_columns_from_dataframe([1, 3])
print("Example 3 finished.")
print(df_3.head(3))

# To add a column.
# (If the Series that is passed is not of the same size, the method will raise an error)
file_manager_4 = FileManager("../data/rating.parquet.gzip")
# Create a column with same row length as df_4, with random numbers.
df_4 = file_manager_4.get_dataframe()
new_column = np.random.randint(0, 10, df_4.shape[0])
# Add the column.
file_manager_4.add_column_to_dataframe("new_column", pd.Series(new_column))
print("Example 4 finished.")
print(file_manager_4.get_dataframe().head(3))

# To save the file.
# (Note that if an extension isn't specified with the keyword argument "extension",
# the manager will save it with the same extension as the original file)
file_manager_5 = FileManager("../data/rating.parquet.gzip")
file_manager_5.read_file(file_manager_5.get_file_path())
# With same extension
file_manager_5.save_dataframe("./test_outputs/file_manager_test_1")
# With a different extension
file_manager_5.save_dataframe("./test_outputs/file_manager_test_1", extension="csv")
print("Example 5 finished. Check test_outputs folder.")

