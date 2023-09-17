import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.functions import *
import yaml
import os
import datetime
from datetime import datetime as dt

# Function to initialize the logger with the date-appended log file name
def init_logger(log_path):
    os.makedirs(log_path, exist_ok=True)
    log_file_name = f"data_pipeline_{dt.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        filename=os.path.join(log_path, log_file_name),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

# Configure logging
log_path = "/home/jovyan/work/digital/logs/"  # Set your log directory path here
init_logger(log_path)
    
# Function to read the configuration from config.yaml
def read_config():
    try:
        with open('config.yaml', 'r') as config_file:
            config = yaml.safe_load(config_file)
        return config
    except FileNotFoundError:
        logging.error("Error: config.yaml file not found.")
        return None
    except yaml.YAMLError as e:
        logging.error(f"Error loading config.yaml: {e}")
        return None

if __name__ == "__main__":
    try:
        # Read the configuration from config.yaml
        config = read_config()

        if config:
            # Accessing configuration parameters
            input_path = config.get('input_path')
            output_path = config.get('output_path')
            log_path = config.get('log_path')
            table_name = config.get('table_name')
            source_file1 = config.get('source_file1')
            source_file2 = config.get('source_file2')

            # Initialize the logger with the log_path from config
            #print(f"log path is :{log_path}")
            #init_logger(log_path)

            # Logging input and output paths
            logging.info(f"Input Path: {input_path}")
            logging.info(f"Output Path: {output_path}")
        else:
            logging.error("Configuration not loaded. Please check the YAML file and its location.")

        # Creating a Spark session with the configured app name
        spark = SparkSession.builder.appName(config["spark"]["app_name"]).getOrCreate()
        # Reading the Characters CSV file into a DataFrame
        logging.info(f"Reading {source_file1}.")
        df_char = spark.read.csv(f"{input_path}/{source_file1}", header=True, inferSchema=True)
        
        # Reading the Character Stats CSV file into a DataFrame
        logging.info(f"Reading {source_file2}.")
        df_stats = spark.read.csv(f"{input_path}/{source_file2}", header=True, inferSchema=True)
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
        dfp_char_stats = df_char_stats.toPandas()
        df_char_stats.write.parquet(f"{output_path}/char_stats_day_dly", mode="overwrite")

        # Print a message to confirm the file has been saved
        logging.info(f"DataFrame saved to Parquet file: {output_path}")

        # Defining the path to your SQL script which will be used for creating Data Objects such as schema and tables
        sql_script_path = "modelling.sql"

        # Reading and executing SQL statements from the modelling script
        logging.info("Reading and executing SQL statements from modelling script.")
        with open(sql_script_path, "r") as script_file:
            sql_statements = script_file.read().split(";")  # Split statements by semicolon

            # Removing if any empty statements in the modelling sql
            logging.info("Removing if any empty statements in the modelling sql.")
            sql_statements = [statement.strip() for statement in sql_statements if statement.strip()]

            # Execute each SQL statement separately
            for statement in sql_statements:
                spark.sql(statement)

        logging.info("Table created Successfully")

        #showing the stats of the table created for data analytics purpose.
        logging.info("Describing the table.")
        spark.sql("describe formatted  db_sil_marvel.char_stats_day_dly").show(truncate=False)

        logging.info("Displaying the first 5 rows of the table.")
        spark.sql("select * from db_sil_marvel.char_stats_day_dly").show(5, truncate=False)

        logging.info("Running data analytics query.")
        spark.sql('select count(1) total_heros,alignment from db_sil_marvel.char_stats_day_dly  group by 2;').show(truncate=False)

        logging.info("Data pipeline completed successfully")

    except Exception as e:
        logging.error(f"Error: {str(e)}")
