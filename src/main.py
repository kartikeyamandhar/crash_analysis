import os
import yaml
import logging
from spark_utils import get_spark_session, get_absolute_path, load_data, write_output, configure_logging
from analysis_utils import perform_analyses

def main():
    # Defining the relative path to the config file
    config_path = os.path.join(os.path.dirname(__file__), '../config/config.yaml')

    # Loading config
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Config file not found at {config_path}")
        raise
    except Exception as e:
        print(f"Error loading config file: {e}")
        raise

    # Converting relative data paths to absolute paths
    for key in config['data_paths']:
        config['data_paths'][key] = get_absolute_path(config['data_paths'][key])
  
    config['output_path'] = get_absolute_path(config['output_path'])
   
    # Configuring logging
    configure_logging(config['output_path'])

    spark = get_spark_session()

    # Avoiding excessive logging
    spark.sparkContext.setLogLevel("WARN")

    # Reading data
    try:
        data_frames = load_data(spark, config['data_paths'])
        charges_df = data_frames['charges']
        damages_df = data_frames['damages']
        endorse_df = data_frames['endorse']
        primary_person_df = data_frames['primary_person']
        restrict_df = data_frames['restrict']
        units_df = data_frames['units']
    except Exception as e:
        logging.error(f"Error reading data: {e}")
        raise

    # Performing analyses and writing results
    try:
        perform_analyses(spark, config, primary_person_df, units_df, damages_df, charges_df)
        print("Analysis is completed, kindly check the output folder.")
    except Exception as e:
        logging.error(f"Error performing analyses: {e}")
        raise

if __name__ == "__main__":
    main()
