-- Creating the database (if it doesn't exist)
CREATE DATABASE IF NOT EXISTS ${db_name};

-- Creating a table from the Parquet file
CREATE TABLE IF NOT EXISTS ${db_name}.${table_name}
USING parquet
OPTIONS (
  path '${parquet_path}'
);

-- Refreshing the table
refresh table ${db_name}.${table_name};