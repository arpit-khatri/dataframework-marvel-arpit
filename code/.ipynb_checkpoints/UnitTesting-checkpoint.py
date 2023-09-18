import unittest
import yaml
import datetime
import argparse
import pandas as pd
import os
import logging

from datetime import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Function which initialize the logger with the date-appended log file name
def init_logger(log_path):
    os.makedirs(log_path, exist_ok=True)
    log_file_name = f"UnitTest_{datetime.datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        filename=os.path.join(log_path, log_file_name),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

class TestCharStats(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
        self.log_path = config["monitoring_path"]
        self.logging_path = config["log_path"]
        self.load_status_table = config["load_status_table"]
        self.absolute_table_name = f"{self.output_path}/{self.table_name}"

        # Initialize logger
        init_logger(self.logging_path)

    # Function to log test results
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
        try:
            # Logging the test start
            logging.info("Starting the Unit Testing to match columns and datatypes.")

            # Loading the current data
            df = self.spark.read.parquet(self.absolute_table_name)
            logging.info(f"Loaded data from {self.table_name}.")

            # Checking if column data types match the configuration
            for column_config in self.columns_config:
                column_name = column_config["name"]
                expected_data_type = column_config["type"]
                actual_data_type = df.schema[column_name].dataType.simpleString()
                if actual_data_type == expected_data_type:
                    self.log_test_result(f"{column_name} data_type", "PASS", "Expected Datatype")
                    
                else:
                    self.log_test_result(f"{column_name} data_type", "FAIL", f"Expected {expected_data_type}, but got {actual_data_type}")

            # Logging the test end
            logging.info("Unit Testing to match columns and datatype.")
        except Exception as e:
            logging.error(f"Error in test_column_data_types: {str(e)}")

    # Function to log daily status
    def daily_log_status(self, results_df):
        try:
            # Log the daily status start
            logging.info("Starting the function to log the job satsus in daily_log_status table.")

            # Converting the test results to write into a file
            results_df.to_csv(f"{self.log_path}/UnitTest.csv", index=False)
            logging.info(f"Saved test results to UnitTest table.")

            # Creating a Spark session and converting the test results to a Spark DataFrame
            spark = SparkSession.builder.appName("UnitTest").getOrCreate()
            UnitTest_df = spark.createDataFrame(results_df)
            print("Unit Test Table status update.")
            UnitTest_df.show(truncate=False)
            logging.info("Creating a dataframe which will have daily_log_status.")

            # Creating a summary DataFrame to store load status
            load_status_df = UnitTest_df \
                .groupBy("Table Name") \
                .agg(F.when(F.expr("array_contains(collect_list(Status), 'FAIL')"), "FAIL").otherwise("PASS").alias("Status")\
                    ,F.lit("UnitTest").alias("Test Type"), F.current_date().alias("Date"))
            
            # Show the load_status_df
            print("Load Daily Status table update.")
            load_status_df.show()

            # Checking if any row in the test results DataFrame has "FAIL" in the "Status" column
            if "FAIL" in load_status_df.select("Status").distinct().rdd.map(lambda x: x[0].upper()).collect():
                logging.warning("Load status set to FAIL due to Unit Testing failures.")

            else:
                logging.info("Load status remains as PASS.")

            # Check if the log file already exists
            if os.path.isfile(f"{self.log_path}/{self.load_status_table}.csv"):
                # Reading the existing load status CSV directly
                existing_load_status_df = spark.read.csv(f"{self.log_path}/{self.load_status_table}.csv", header=True, inferSchema=True)
                final_load_status_df = existing_load_status_df.union(load_status_df).distinct()
                final_load_status_dfp = final_load_status_df.toPandas()
                final_load_status_dfp.to_csv(f"{self.log_path}/{self.load_status_table}.csv", index=False)
                logging.info(f"Appended the today's load status to table load_status.")
            else:
                # if the file doesn't exist, creating it with the current load status
                final_load_status_df = load_status_df
                final_load_status_dfp = final_load_status_df.toPandas()
                final_load_status_dfp.to_csv(f"{self.log_path}/{self.load_status_table}.csv", mode='a', index=False, header=not os.path.exists(f"{self.log_path}/load_status.csv"))
                logging.info(f"Created {self.log_path}/{self.load_status_table}.csv with current load status")

            # Log the daily status end
            logging.info("Completed updating the daily_log_status for today's job status.")
        except Exception as e:
            logging.error(f"Error in daily_log_status: {str(e)}")

if __name__ == '__main__':
    try:
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

        # Calling the daily_log_status function with test_results as an argument
        test_instance = TestCharStats()
        test_instance.daily_log_status(results_df)
        logging.info("UnitTesting Job completed successfully.")
    except Exception as e:
        # Handling fatal errors
        error_message = str(e)
        logging.error(f"UnitTesting Job failed: {error_message}")