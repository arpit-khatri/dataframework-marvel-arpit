import unittest
import yaml
import datetime
import argparse
import pandas as pd
import os
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, upper
from pyspark.sql.window import Window
from pyspark.sql import functions as F



# Define command-line arguments
#parser = argparse.ArgumentParser(description="Unit test script for Spark DataFrame schema and data validation")
#parser.add_argument("--table_name", required=True, help="Path to the target table")

class TestCharStats(unittest.TestCase):
    def setUp(self):
        # Parse command-line arguments
        #args = parser.parse_args()
        #self.table_name = args.table_name

        # Loading configuration from config.yaml
        with open("config.yaml", "r") as config_file:
            config = yaml.safe_load(config_file)

        # Create a Spark session
        self.spark = SparkSession.builder.appName("TestCharStats").getOrCreate()

        # Set the log level to ERROR or FATAL
        self.spark.sparkContext.setLogLevel("ERROR")  # You can change "ERROR" to "FATAL" if needed

        # Assign configuration variables
        self.columns_config = config["columns"]
        self.table_name = config["table_name"]
        self.output_path = config["output_path"]
        self.absolute_table_name =f"{self.output_path}/{self.table_name}"

    def tearDown(self):
        # Stop the Spark session
        self.spark.stop()

    def log_test_result(self, test_name, status, reason=""):
        timestamp = datetime.datetime.now()
        # Creating a dictionary for the test result
        result = {
            "Table Name": self.table_name,
            "Test Name": test_name,
            "Status": status,
            "Reason": reason,
            "Timestamp": timestamp
        }
        # Append the result to the test_results list
        test_results.append(result)

    def test_column_data_types(self):
        # Loading the current data
        df = self.spark.read.parquet(self.absolute_table_name)

        # Check if column data types match the configuration
        for column_config in self.columns_config:
            column_name = column_config["name"]
            expected_data_type = column_config["type"]
            actual_data_type = df.schema[column_name].dataType.simpleString()
            if actual_data_type == expected_data_type:
                self.log_test_result(f"{column_name} data_type", "PASS")
            else:
                self.log_test_result(f"{column_name} data_type", "FAIL", f"Expected {expected_data_type}, but got {actual_data_type}")

if __name__ == '__main__':
    # Creating an empty list to store test results
    test_results = []

    # Creating a test suite
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestCharStats)

    # Running the tests
    test_runner = unittest.TextTestRunner()

    # Run each test and log the results
    for test_case in test_suite:
        test_result = test_runner.run(test_case)

    # Converting the test results to write into a file
    results_df = pd.DataFrame(test_results)
    results_df.to_csv("UnitTesting.csv", index=False)

    # Create a Spark session and converting the test results to spark dataframe
    spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
    #schema = ["Table Name", "Status", "Test Type", "Date"]

    UnitTest_df = spark.createDataFrame(results_df)
    
    
     # Create a summary DataFrame to store load status
    load_status_df = UnitTest_df.groupby("Table Name")\
        .agg(F.max("Status").alias("Status"), F.lit("UnitTesting").alias("Test Type"), F.current_date().alias("Date"))
    # Check if any row in the test results DataFrame has "FAIL" in the "Status" column
    if "FAIL" in load_status_df.select("Status").distinct().rdd.map(lambda x: x[0].upper()).collect():
        # Set the status in the summary DataFrame to "FAIL"
        load_status_df = load_status_df.withColumn("Status", F.lit("FAIL"))
    
    # Define the file path for the load status CSV
    csv_file_path = "load_status"
    load_status_df.show
    
    # Check if the file already exists
    #if os.path.isfile(f"work/digital/logs/{csv_file_path}.csv"):
    if os.path.isfile(f"{csv_file_path}.csv"):
        ## Read the existing load status CSV directly
        existing_load_status_df = spark.read.csv(f"{csv_file_path}.csv", header=True, inferSchema=True)
        final_load_status_df = existing_load_status_df.union(load_status_df).distinct()
        final_load_status_dfp=final_load_status_df.toPandas()
        final_load_status_dfp.to_csv("load_status.csv",index=False)
    else:
        # The file doesn't exist, create it with the current load status
        final_load_status_df = load_status_df
        final_load_status_dfp=final_load_status_df.toPandas()
        final_load_status_dfp.to_csv("load_status.csv", mode='a', index=False, header=not os.path.exists("load_status.csv"))
