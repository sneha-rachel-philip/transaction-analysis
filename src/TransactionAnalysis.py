import json

from pyspark.sql import SparkSession

from src.ExecuteOperations import read_data, transform_data, write_data


# Function to load configuration
def load_config(config_path):
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("TransactionAnalysis")
             .master("local[*]")
             .enableHiveSupport()
             .getOrCreate())

    config = load_config('config/config.json')
    input_file = config['input_file']
    output_path = config['output_path']

    # Reading the data
    input_df = read_data(spark, input_file)

    # Transforming the data
    transformed_df = transform_data(input_df)

    # Display the transformed data (optional)
    transformed_df.show()

    # Writing the data to the specified output path
    write_data(transformed_df, output_path)

    # Stop the Spark session
    spark.stop()
