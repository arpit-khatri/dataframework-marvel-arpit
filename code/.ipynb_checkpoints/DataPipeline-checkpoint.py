import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
import datetime
from datetime import datetime as dt
import yaml

def init_logger(log_path):
    os.makedirs(log_path, exist_ok=True)
    log_file_name = f"data_pipeline_{dt.now().strftime('%Y%m%d')}.log"  # Include script name in the log file name
    log_file_path = os.path.join(log_path, log_file_name)  # Full path to the log file

    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create a single formatter for both the file handler and the console handler
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Create a handler for writing log messages to a file
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)

    # Create a handler for writing log messages to the console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
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

def execute_sql_script(sql_script_path, spark, db_name, table_name, parquet_path):
    """
    Executes SQL statements from a SQL script and return the results.

    Args:
        sql_script_path (str): Path to the SQL script.
        spark (SparkSession): SparkSession object.
        db_name (str): Name of the database.
        table_name (str): Name of the table.
        parquet_path (str): Path to the Parquet file.

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
            input_path = config.get('input_path')
            output_path = config.get('output_path')
            logging_path = config.get('log_path')
            code_path = config.get('code_path')
            source_file1 = config.get('source_file1')
            source_file2 = config.get('source_file2')
            db_name = config.get('database')['db_name'] 
            table_name = config.get('database')['table_name']
            ddl_script = config.get('database')['ddl_script']
            parquet_path = os.path.join(output_path, table_name)
            
            # Initialize the logger with the log_path from config
            #print(f"log path is :{log_path}")
            init_logger(logging_path)

            # Logging input and output paths
            logging.info(f"Input Path is {input_path}")
            logging.info(f"Target table Output Path is {output_path}")
        else:
            logging.error("Configuration not loaded. Please check the YAML file and its location.")
 
        # Creating a Spark session with the configured app name
        spark = SparkSession.builder.appName("DataPipeline").getOrCreate()

        # Reading the Characters CSV file into a DataFrame
        logging.info(f"Reading {source_file1}.")
        df_char = spark.read.csv(os.path.join(input_path, source_file1), header=True, inferSchema=True)
        
        # Reading the Character Stats CSV file into a DataFrame
        logging.info(f"Reading {source_file2}.")
        df_stats = spark.read.csv(os.path.join(input_path, source_file2), header=True, inferSchema=True)
        df_stats = df_stats.withColumnRenamed("Name","name")

        # Joining the character & Character Stats files
        logging.info("Joining character and Character Stats DataFrames.")
        df_char_stats = df_char.join(df_stats, on="name", how="inner")

        # Adding the audit columns
        logging.info("Adding audit columns.")
        df_char_stats = df_char_stats.withColumn("batch_id", lit("101"))
        df_char_stats = df_char_stats.withColumn("load_date", current_timestamp().cast("string"))

        # Saving the DataFrame to a Parquet file
        logging.info("Saving DataFrame to Parquet file.")
        df_char_stats.write.parquet(os.path.join(output_path, "char_stats_day_dly"), mode="overwrite")

        # Print a message to confirm the file has been saved
        logging.info(f"DataFrame saved to Parquet file: {output_path}")

        # Defining the path to SQL script which will be used for creating Data Objects such as schema and tables
        sql_script_path = os.path.join(code_path, ddl_script)
        
        # Execute the DDL SQL script and get the results
        logging.info(f"Running the SQL Script: {ddl_script}")
        script_results = execute_sql_script(sql_script_path, spark, db_name, table_name, parquet_path)
        logging.info(f"Target Table {db_name}.{table_name} created Successfully.")
        logging.info("Data pipeline completed successfully.")

    except Exception as e:
        logging.error(f"Error: {str(e)}", exc_info=True)
