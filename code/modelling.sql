-- Create the database (if it doesn't exist)
CREATE DATABASE IF NOT EXISTS db_sil_marvel;

-- Create a managed table from the Parquet file
CREATE TABLE IF NOT EXISTS db_sil_marvel.char_stats_day_dly
USING parquet
OPTIONS (
  path '/home/jovyan/work/digital/target/char_stats_day_dly/'
);


refresh table db_sil_marvel.char_stats_day_dly;