from pyspark.sql import SparkSession
import os
import logging

def get_spark_session():
    return SparkSession.builder.appName("BCG Case Study").getOrCreate()

def read_csv(spark, path):
    return spark.read.csv(path, header=True, inferSchema=True)

def get_absolute_path(relative_path):
    base_path = os.path.dirname(os.path.abspath(__file__))
    absolute_path = os.path.join(base_path, relative_path)
    return absolute_path

def write_output(output_path, filename, data):
    import json
    with open(os.path.join(output_path, f"{filename}.json"), 'w') as f:
        json.dump(data, f, indent=4)

def load_data(spark, data_paths):
    data_frames = {}
    for key, path in data_paths.items():
        data_frames[key] = read_csv(spark, path)
    return data_frames

def configure_logging(output_path):
    # Ensuring the output directory exists
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    logging.basicConfig(
        filename=os.path.join(output_path, "logs.log"),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filemode='w'
    )
