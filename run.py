from file_manager import FileManager
from feature_extractor import FeatureExtractor
import time

"""
Run this script to extract the features from the datastore.
The current settings are as follows:
- Original data file: rating.parquet.gzip
- Outputs:
    - all_features.parquet.gzip: File containing the original DataFrame plus the extracted features.
    - extracted_features.parquet.gzip: File containing the extracted features only.
    
TODO: Write the input, output names and parameter settings into a JSON so they can be easily changed and the same
run.py script can be run each time. 
"""
print("Start")
initial_time = time.time()
file_manager = FileManager("./data/rating.parquet.gzip")
df = file_manager.get_dataframe()

feature_extractor = FeatureExtractor(df)
feature_extractor.extract_nb_previous_ratings()
feature_extractor.extract_avg_ratings_previous()

feature_extractor.save_dataframe("./data/extracted_features.parquet.gzip", save_only_added_features=True)
feature_extractor.save_dataframe("./data/all_features.parquet.gzip")
print(f"Finished in {time.time()-initial_time}s")

