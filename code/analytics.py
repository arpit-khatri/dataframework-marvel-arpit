import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.functions import *
import datetime
from datetime import datetime as dt
import yaml

def init_logger(log_path):
    os.makedirs(log_path, exist_ok=True)
    log_file_name = f"Analytics_{dt.now().strftime('%Y%m%d')}.log"  # Include script name in the log file name
    log_file_path = os.path.join(log_path, log_file_name)  # Full path to the log file

    # Creating a logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Creating a single formatter for both the file handler and the console handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Creating a handler for writing log messages to a file
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Creating a handler for writing log messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Adding the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# Function to read the configuration from config.yaml
def read_config():
    try:
        config_file_path = 'config.yaml'
        if os.path.exists(config_file_path):
            with open(config_file_path, 'r') as config_file:
                config = yaml.safe_load(config_file)
            return config
        else:
            logging.error("Error: config.yaml file not found.")
            return None
    except yaml.YAMLError as e:
        logging.error(f"Error loading config.yaml: {e}")
        return None

def execute_sql_script(sql_script_path, spark, db_name, table_name, parquet_path ):
    """
    Executes SQL statements from a SQL script and return the results.

    Args:
        sql_script_path (str): Path to the SQL script.
        spark (SparkSession): SparkSession object.
        db_name (str): Name of the database.
        table_name (str): Name of the table.
        parquet_path (str): Path to the Parquet file.
        script_name (str): Name of the script.

    Returns:
        list: List of DataFrames containing the results of SQL statements.
    """
    try:
        # Read and execute the SQL statements from the script
        logging.info(f"Reading and executing SQL statements from {sql_script_path}.")
        with open(sql_script_path, "r") as script_file:
            script_content = script_file.read()  # Read the content of the file

            # Replace placeholders with actual values
            script_content = script_content.replace("${db_name}", db_name)
            script_content = script_content.replace("${table_name}", table_name)
            script_content = script_content.replace("${parquet_path}", parquet_path)
            sql_statements = script_content.split(";")

            # Remove any empty statements in the SQL script
            sql_statements = [statement.strip() for statement in sql_statements if statement.strip()]

            # Execute each SQL statement separately
            results = []
            for statement in sql_statements:
                if statement:
                    result = spark.sql(statement)
                    results.append(result)
                    
                    # Check if script_name contains "analytics" to determine whether to show the result
                    if "analytics" in sql_script_path:
                        result.show(50,truncate=False)  # Display the result
                        
            return results

    except Exception as e:
        logging.error(f"Error: {str(e)}", exc_info=True)
        return []


if __name__ == "__main__":
    try:
        # Read the configuration from config.yaml
        config = read_config()

        if config:
            # Accessing the input_path from the config
            output_path = config.get('output_path')
            logging_path = config.get('log_path')
            code_path = config.get('code_path')
            db_name = config.get('database')['db_name'] 
            table_name = config.get('database')['table_name']
            ddl_script = config.get('database')['ddl_script']
            analytics_script = config.get('database')['analytics_script']
            parquet_path = os.path.join(output_path, table_name)
            
            # Initialize the logger with the log_path from config
            init_logger(logging_path)

        else:
            logging.error("Configuration not loaded. Please check the YAML file and its location.")
 
        # Creating a Spark session with the configured app name
        spark = SparkSession.builder.appName("DataAnalytics").getOrCreate()

        # Defining the path to SQL script which will be used for creating Data Objects such as schema and tables
        sql_ddl_script_path = os.path.join(code_path, ddl_script)
        sql_analytics_script_path = os.path.join(code_path, analytics_script)
        
        # Execute the SQL script and get the results
        logging.info(f"Running the SQL Scripts: {analytics_script}")
        script_results = execute_sql_script(sql_ddl_script_path, spark, db_name, table_name, parquet_path)
        script_results = execute_sql_script(sql_analytics_script_path, spark, db_name, table_name, parquet_path)
            
        logging.info("Data Analytics script completed successfully.")

    except Exception as e:
        error_message = str(e)
        logging.error(f"Data Analytics Job failed: {error_message}")
