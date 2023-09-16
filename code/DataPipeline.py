from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.functions import *

import yaml

# Function to read the configuration from config.yaml
def read_config():
    try:
        with open('config.yaml', 'r') as config_file:
            config = yaml.safe_load(config_file)
        return config
    except FileNotFoundError:
        print("Error: config.yaml file not found.")
        return None
    except yaml.YAMLError as e:
        print(f"Error loading config.yaml: {e}")
        return None

if __name__ == "__main__":
    # Read the configuration from config.yaml
    config = read_config()

    if config:
        # Access the input_path from the config
        input_path = config.get('input_path')
        output_path = config.get('output_path')

        # Print the input_path (you can use it in your PySpark pipeline)
        print(f"Input Path: {input_path}")
        print(f"Output Path: {output_path}")
    else:
        print("Configuration not loaded. Please check the YAML file and its location.")

    
    # Create a Spark session with the configured app name
    spark = SparkSession.builder.appName(config["spark"]["app_name"]).getOrCreate()

# Read the CSV file into a DataFrame
df_char = spark.read.csv(f"{input_path}/characters.csv", header=True, inferSchema=True)
#df_char.show(5)

# Read the CSV file into a DataFrame
df_stats = spark.read.csv(f"{input_path}/characters_stats.csv", header=True, inferSchema=True)
df_stats = df_stats.withColumnRenamed("Name","name")
#df_stats.show(5)

df_char_stats = df_char.join(df_stats, on="name", how="inner")
# Add audit columns
df_char_stats = df_char_stats.withColumn("batch_id", lit("101"))
df_char_stats = df_char_stats.withColumn("load_date", current_timestamp().cast("string"))
#df_char_stats.show(5)

# Save the DataFrame to a Parquet file
dfp_char_stats = df_char_stats.toPandas()
try:
    df_char_stats.write.parquet(f"{output_path}/char_stats_day_dly", mode="overwrite")
    # Print a message to confirm the file has been saved
    print(f"DataFrame saved to Parquet file: {output_path}")
except Exception as e:
    print(f"Error writing DataFrame to Parquet: {str(e)}")


# Define the path to your SQL script
sql_script_path = "modelling.sql"

# Read and execute SQL statements from the script
with open(sql_script_path, "r") as script_file:
    sql_statements = script_file.read().split(";")  # Split statements by semicolon

    # Remove empty statements
    sql_statements = [statement.strip() for statement in sql_statements if statement.strip()]

    # Execute each SQL statement separately
    for statement in sql_statements:
        spark.sql(statement)
    # Save the SparkSession to a file
#spark.sparkContext.setCheckpointDir("path/to/checkpoint")  # Set checkpoint directory
#spark.save()
print("Table created Successfully")


#showing the stats of the table created
spark.sql("describe formatted  db_sil_marvel.char_stats_day_dly").show(truncate=False)

spark.sql("select * from db_sil_marvel.char_stats_day_dly").show(5,truncate=False)

spark.sql('select count(1) total_heros,alignment from db_sil_marvel.char_stats_day_dly  group by 2;').show(truncate=False)
