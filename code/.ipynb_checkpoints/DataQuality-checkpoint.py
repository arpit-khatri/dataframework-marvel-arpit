import unittest
import yaml
import datetime
import pandas as pd
from pyspark.sql import SparkSession

class TestCharStats(unittest.TestCase):
    def setUp(self):
        # Loading configuration from config.yaml
        with open("config.yaml", "r") as config_file:
            config = yaml.safe_load(config_file)

        # Create a Spark session
        #self.spark = SparkSession.builder.appName("TestCharStats").getOrCreate()

        # Create a Spark session with a custom configuration
        self.spark = SparkSession.builder \
            .appName("TestCharStats") \
            .getOrCreate()
        # Set the log level to ERROR or FATAL
        self.spark.sparkContext.setLogLevel("ERROR")  # You can change "ERROR" to "FATAL" if needed
        
        # Assign configuration variables
        self.table_name = config["table_name"]
        self.primary_key_columns = config["primary_key_columns"]
        self.threshold_percentage = config["threshold_percentage"]

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

    def test_primary_key(self):
        # Loading the current data
        df = self.spark.read.parquet("/home/jovyan/work/digital/target/char_stats/")  

        # Checking primary key columns to make sure no duplicates
        num_rows = df.count()
        num_distinct_rows = df.select(*self.primary_key_columns).distinct().count()
        #self.assertEqual(num_rows, num_distinct_rows)
        if num_rows == num_distinct_rows:
            self.log_test_result("test_primary_key", "Pass")
        else:
            self.log_test_result("test_primary_key", "Fail", "Duplicate primary key values found")

    def test_count_increase(self):
        # Loading the today's and previous day data for the comparision of countyour DataFrames for previous and current loads
        df_previous = self.spark.read.parquet("/home/jovyan/work/digital/target/char_stats/")
        df_current = self.spark.read.parquet("/home/jovyan/work/digital/target/char_stats/")  # Replace with the actual path

        # Calculating the count of increased percentage
        count_previous = df_previous.count()
        count_current = df_current.count()
        increase_percentage = (count_current - count_previous) / count_previous * 100

        if increase_percentage >= self.threshold_percentage:
            self.log_test_result("test_count_increase", "Pass")
        else:
            self.log_test_result("test_count_increase", "Fail", "Count increase percentage is below threshold")

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

    # Write the results to a CSV file
    results_df.to_csv("test_results.csv", index=False)
