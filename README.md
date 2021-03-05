# Kueski Data Pipeline
Simple pipeline to extract 2 different features from the MovieLens dataset.

READ FIRST: This repo only contains the code. To get the all of the files including the Input Datasets (parquet and csv) and output Datasets, please download the following [zip](https://drive.google.com/file/d/1WsnONcNCGpBWbfRazLJ88EKHW9CNgaj0/view?usp=sharing). If you wish to only see an pipeline example using the code, please open the *pipeline_jupyter.ipynb* notebook. Finally if you want to see only the output files produced by the code, download them [here](https://drive.google.com/file/d/1epMuslS3uNRQse9CxAAtLbB7FoReIIEK/view?usp=sharing)

## Demo
Please run the *run.py* script to test the pipeline. 
- Make sure the necessary dependencies are installed (they are specified in the requirements.txt).
- Please make sure you have the dataset in parquet format, or download it from [here](https://drive.google.com/file/d/1MsujXhBpXXvp2apTXA4ruaSdEmalKC4k/view?usp=sharing)). 

Alternatively, you can test the pipeline using the *pipeline_jupyter.ipynb* Notebook provided. 
- Make sure you upload the *ratings.parquet.gzip* file stored in the *./data/* folder [here](https://drive.google.com/file/d/1MsujXhBpXXvp2apTXA4ruaSdEmalKC4k/view?usp=sharing)).

## Overview
kueski_data_pipeline is a small python app to extract 2 features from the MovieLens Dataset. 2 Classes are provided:
- FileManager: Class that manages the reading and handling of files. This class was created to simplify access to the dataset, and to potentially add more features to the dataset in the future, or extract and save required features in a new dataset. Supported files are CSV, PARQUET and JSON. See the file_manager_examples.py script for examples and features added to this class.
- FeatureExtractor: Class that manages the extraction of features from the dataset. It can extract features independently and return them as Pandas Series, or it can add them to the DataFrame provided. It can also save the results in CSV, PARQUET and JSON. See the feature_extractor_examples.py script for examples and features added to this class.

## Pipeline
The steps followed on the *run.py* and the *pipeline_jupyter.ipynb* scripts were the following:
1. The original dataset was converted to Parquet format using the csv_parquet_converter. This was done to save memory and some of the loading time. The hdf5 format was originally considered to make the loading times even faster, but parquet was chosen instead because it can handle DateTime types.
2. The dataset is sorted by userId and timestamp (in that order) and timestamp strings are converted into pandas timestamps.
3. The nb_previous_ratings Feature Series is obtained by grouping the userId column and creating a new rank column based on the "timestamp" column. This was done the way it was implemented in the original pipeline. Tests were made to handle the data using numpy, but the results didn't show significant speed improvements, so numpy was discarted.
4. The avg_ratings_previous Feature Series is obtained. First, a cumulative sum of the subgroups is calculated (the subgroups are grouped by userId). Then, the cumulative sum is used alongisde the nb_previous_ratings column to compute the averages at each row. Because this is showing the average at each point including the current data point, the Series is shifted, so that each row contains the average of the former rows without the current data point. Finally, np.nans are added to the beginning of each user group.
5. Finally, 2 dataframes are saved to the *./data/* path. The all_features.parquet.gzip file contains the original DataFrame plus the added feature columns. The extracted_features.parquet.gzip contains only the new features extracted.

The whole process takes around 130-150 seconds to compute (<3min), which shows a significant improvement in performance compared to the original data pipeline. 

Several improvements could still be made, which are listed now:
- The number datatypes are all float64, which requires a lot of space. The ratings column could be stored as a float16 dtype.
- Numpy is supposed to be faster, specially when vectorizing functions. Perhaps with more time a numpy-based pipeline could improve performance further.
