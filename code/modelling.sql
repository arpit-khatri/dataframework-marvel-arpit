-- Create the database (if it doesn't exist)
CREATE DATABASE IF NOT EXISTS db_sil_marvel;

-- Use the database
USE db_sil_marvel;

-- Create a managed table from the Parquet file
CREATE TABLE IF NOT EXISTS char_stats_day_dly
USING parquet
OPTIONS (
  path 'path_to_parquet_file'
);

-- Register the table in Spark
CREATE OR REPLACE TEMPORARY VIEW char_stats_table AS
SELECT * FROM char_stats
;
