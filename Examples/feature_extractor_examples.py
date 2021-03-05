# This script contains some examples on how to use the FeatureExtractor class
from feature_extractor import FeatureExtractor
from file_manager import FileManager

# To create an instance with a DataFrame
file_manager = FileManager("../data/rating.parquet.gzip")
df = file_manager.get_dataframe()
feature_extractor = FeatureExtractor(df)
print("Example 1 finished. FeatureExtractor created.")

# To only extract the nb_previous_ratings Series.
# (Note that if add=False, nb_previous_ratings is only returned and
# will not be stored on the original DataFrame.)
feature_series = feature_extractor.extract_nb_previous_ratings(add=False)
print("Example 2 finished.")
print(feature_series[:3])

# To only extract the avg_ratings_previous Series.
# (Note that if add=False, avg_ratings_previous is only returned and
# will not be stored on the original DataFrame. The nb_previous_ratings will be calculated
# to get the features, but will not be stored.)
feature_series = feature_extractor.extract_avg_ratings_previous(add=False)
print("Example 3 finished.")

# To add the nb_previous_ratings and avg_ratings_previous Series to the DataFrame.
feature_extractor.extract_nb_previous_ratings()
feature_extractor.extract_avg_ratings_previous()
print("Example 4 finished. Both features added to original DataFrame")

# To save only the extracted features on a new file.
# (Note that the file name is specified in the path name, there is no need to specify it explicitly.)
feature_extractor.save_dataframe("./test_outputs/test_added_features_only.gzip", save_only_added_features=True)
print("Example 5 finished. Check test_outputs folder.")

# To save the dataframe with the added features.
feature_extractor.save_dataframe("./test_outputs/test_all_features.gzip")
print("Example 6 finished. Check test_outputs folder.")
