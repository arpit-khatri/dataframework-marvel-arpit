import unittest
import yaml
import datetime
import pandas as pd
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper
from pyspark.sql.window import Window
from pyspark.sql import functions as F

class TestCharStats(unittest.TestCase):
    def setUp(self):
        # Loading configuration from config.yaml
        with open("config.yaml", "r") as config_file:
            config = yaml.safe_load(config_file)
            
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

    def test_primary_key(self):
        # Loading the current data
        df = self.spark.read.parquet(self.absolute_table_name)  

        # Checking primary key columns to make sure no duplicates
        num_rows = df.count()
        num_distinct_rows = df.select(*self.primary_key_columns).distinct().count()
        if num_rows == num_distinct_rows:
            self.log_test_result("test_primary_key", "PASS")
        else:
            count_duplicate_key = num_rows - num_distinct_rows
            self.log_test_result("test_primary_key", "FAIL", f"total of {count_duplicate_key} Duplicate primary key values found.")

    def test_count_increase(self):
        # Loading the today's and previous day data for the comparision of countyour DataFrames for previous and current loads
        df_previous = self.spark.read.parquet(self.absolute_table_name)
        df_current = self.spark.read.parquet(self.absolute_table_name)

        # Calculating the count of increased percentage
        count_previous = df_previous.count()
        count_current = df_current.count()
        increase_percentage = (count_current - count_previous) / count_previous * 100

        if increase_percentage >= self.threshold_percentage:
            self.log_test_result("test_count_increase", "FAIL", f"Count_increase percentage is more than threshold")
        else:
            self.log_test_result("test_count_increase", "PASS", "Count_increase percentage is below threshold")

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
        
   # Create a Spark session
    spark = SparkSession.builder.appName("DataQuality").getOrCreate()
    
    schema = ["Table Name", "Test Type", "Status", "Reason", "Date"]
    # Converting the test results to spark dataframe
    results_df = pd.DataFrame(test_results)
    results_df.to_csv("DataQuality.csv", index=False)
    # Write the individual Data Quality results to a CSV file
    data_quality_df = spark.createDataFrame(results_df, schema=schema) 
    data_quality_df.show()
    
     # Create a summary DataFrame to store load status
    #load_status_df = data_quality_df.groupby("Table Name")\
     #   .agg(F.max("Status").alias("Status"), F.lit("Data Quality Checks").alias("Test Type"), F.date_sub(F.current_date(), 1).alias("Date"))
    load_status_df = data_quality_df.groupby("Table Name")\
        .agg(F.max("Status").alias("Status"), F.lit("Data Quality Checks").alias("Test Type"), F.current_date().alias("Date"))


    # Check if any row in the test results DataFrame has "FAIL" in the "Status" column
    if "FAIL" in load_status_df.select("Status").distinct().rdd.map(lambda x: x[0].upper()).collect():
        # Set the status in the summary DataFrame to "FAIL"
        load_status_df = load_status_df.withColumn("Status", F.lit("FAIL"))

    # Define the file path for the load status CSV
    csv_file_path = "load_status"
    
    # Check if the file already exists
    #if os.path.isfile(f"work/digital/logs/{csv_file_path}.csv"):
    if os.path.isfile(f"{csv_file_path}.csv"):
        print(f"{csv_file_path}.csv")
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
    
