-- Create the database (if it doesn't exist)
CREATE DATABASE IF NOT EXISTS db_sil_marvel;

-- Use the database
USE db_sil_marvel;

-- Create a managed table from the Parquet file
CREATE TABLE IF NOT EXISTS char_stats_day_dly
USING parquet
OPTIONS (
  path '/home/jovyan/work/digital/target/char_stats.parquet'
);
