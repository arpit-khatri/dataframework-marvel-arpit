import unittest
import yaml
import datetime
import pandas as pd
import os
import logging

from datetime import datetime as dt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Function to initialize the logger with the date-appended log file name
def init_logger(log_path):
    os.makedirs(log_path, exist_ok=True)
    log_file_name = f"DataQuality_{datetime.datetime.now().strftime('%Y%m%d')}.log"
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
        self.spark.sparkContext.setLogLevel("ERROR")

        # Assign configuration variables
        self.table_name = config["table_name"]
        self.primary_key_columns = config["primary_key_columns"]
        self.threshold_percentage = config["threshold_percentage"]
        self.output_path = config["output_path"]
        self.log_path = config["monitoring_path"]
        self.logging_path = config["log_path"]
        self.absolute_table_name = f"{self.output_path}/{self.table_name}"
        self.load_status_table = config["load_status_table"]
        
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

    def test_primary_key(self):
        try:
            # Log the test start
            logging.info("Starting the process to to test primary_key duplicate check.")

            # Loading the current data
            df = self.spark.read.parquet(self.absolute_table_name)
            logging.info(f"Loaded today's data from {self.table_name}.")

            # Checking primary key columns to make sure no duplicates
            num_rows = df.count()
            num_distinct_rows = df.select(*self.primary_key_columns).distinct().count()
            if num_rows == num_distinct_rows:
                self.log_test_result("test_primary_key", "PASS")
                logging.info("Duplicate Primary key test passed.")
            else:
                count_duplicate_key = num_rows - num_distinct_rows
                self.log_test_result("test_primary_key", "FAIL", f"Total of {count_duplicate_key} duplicate primary key values found")
                logging.warning(f"Primary key test failed: Total of {count_duplicate_key} duplicate primary key values found")

            # Log the test end
            logging.info("Completed the Primary key duplicate test.")
        except Exception as e:
            logging.error(f"Error in test_primary_key: {str(e)}")

    def test_count_increase(self):
        try:
            # Log the test start
            logging.info("Starting the data quality check to monitor count_increase in today's load.")

            # Loading the today's and previous day data for the comparison of count
            df_previous = self.spark.read.parquet(self.absolute_table_name)
            df_current = self.spark.read.parquet(self.absolute_table_name)
            logging.info(f"Loaded data from yesterday data from table {self.table_name} for count comparison.")

            # Calculating the count increase percentage
            count_previous = df_previous.count()
            count_current = df_current.count()
            increase_percentage = (count_current - count_previous) / count_previous * 100

            if increase_percentage >= self.threshold_percentage:
                self.log_test_result("test_count_increase", "FAIL", f"Count increase percentage ({increase_percentage}%) is more than threshold ({self.threshold_percentage}%)")
                logging.warning(f"Count increase test failed: Count increase percentage ({increase_percentage}%) is more than threshold ({self.threshold_percentage}%)")
            else:
                self.log_test_result("test_count_increase", "PASS", f"Count increase percentage  is below threshold ({self.threshold_percentage}%)")
                logging.info(f"Count increase test passed: Count increase percentage is below threshold ({self.threshold_percentage}%)")

            # Log the test end
            logging.info("Completed the data check for count_increase in today's load.")
        except Exception as e:
            logging.error(f"Error in data count_increase process: {str(e)}")

    def daily_log_status(self, results_df):
        try:
            # Log the daily status start
            logging.info("Starting the function to log the job satsus in daily_log_status table.")

            # Write the individual Data Quality results to a CSV file
            results_df.to_csv(f"{self.log_path}/DataQuality.csv", index=False)
            logging.info(f"Saved test results of  DataQuality table.")

            spark = SparkSession.builder.appName("DataQuality").getOrCreate()
            schema = ["Table Name", "Test Type", "Status", "Reason", "Date"]
            data_quality_df = spark.createDataFrame(results_df, schema=schema)
            print("Data Quality Check Table update.")
            data_quality_df.show(truncate=False)
            logging.info("Displayed Data Quality results.")

            # Creating a summary DataFrame to store load status
            load_status_df = data_quality_df \
                .groupBy("Table Name") \
                .agg(F.when(F.expr("array_contains(collect_list(Status), 'FAIL')"), "FAIL").otherwise("PASS").alias("Status")\
                    ,F.lit("DataQuality").alias("Test Type"), F.current_date().alias("Date"))
            
            # Show the load_status_df
            print("Load Daily Status table update.")
            load_status_df.show()
            
            # Checking if any row in the test results DataFrame has "FAIL" in the "Status" column
            if "FAIL" in load_status_df.select("Status").distinct().rdd.map(lambda x: x[0].upper()).collect():
                logging.warning("Load status set to FAIL due to Data Quality test failures.")
            else:
                logging.info("Load status remains as PASS.")

            # Check if the log file already exists
            if os.path.isfile(f"{self.log_path}/{self.load_status_table}.csv"):
                # Reading the existing load status CSV directly
                existing_load_status_df = spark.read.csv(f"{self.log_path}/{self.load_status_table}.csv", header=True, inferSchema=True)
                final_load_status_df = existing_load_status_df.union(load_status_df).distinct()
                final_load_status_dfp = final_load_status_df.toPandas()
                final_load_status_dfp.to_csv(f"{self.log_path}/{self.load_status_table}.csv", index=False)
                logging.info(f"Appended load status to existing load_status table.")
            else:
                # if the file doesn't exist, creating it with the current load status
                final_load_status_df = load_status_df
                final_load_status_dfp = final_load_status_df.toPandas()
                final_load_status_dfp.to_csv(f"{self.log_path}/{self.load_status_table}.csv", mode='a', index=False, header=not os.path.exists(f"{self.log_path}/load_status.csv"))
                logging.info(f"Created load_status with current load status")

            # Log the daily status end
            logging.info("Completed the daily_log_status")
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
        logging.info("Data Quality Job completed successfully.")
    except Exception as e:
        # Handling fatal errors
        error_message = str(e)
        logging.error(f"Data Quality Job failed: {error_message}")