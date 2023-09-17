# Data Pipeline Job

The Data Pipeline Job is a script that performs data transformation and loads data into Parquet format. Additionally, it calls a modeling script to create a database and an external Spark SQL table. The job is configured using a `config.yaml` file, which specifies parameters such as source path, target path, database name, and table name.

## Prerequisites
Before running the Data Pipeline Job, make sure you have the following prerequisites installed and set up:

- Apache Spark
- Python
- PySpark
- [config.yaml](config.yaml) file with the necessary configuration parameters.

## Configuration
The `config.yaml` file contains the following configuration parameters:

- `source_path`: The path to the source data.
- `target_path`: The path where the transformed data will be saved in Parquet format.
- `database_name`: The name of the database where the external Spark SQL table will be created.
- `table_name`: The name of the external Spark SQL table.
- Other job-specific parameters.

Make sure to update the `config.yaml` file with your desired configuration.

## Running the Data Pipeline Job
To run the Data Pipeline Job, follow these steps:
1. Update the `config.yaml` file with the appropriate configuration parameters.
2. Run the following command to execute the Data Pipeline Job:

   python DataPipeline.py


## Unit Testing Job

The Unit Testing Job (`UnitTesting.py`) is an essential part of the data pipeline process. It runs once the table is created for today's load. The primary purpose of this job is to validate the quality of the data by performing unit tests.

### Unit Testing Process

The Unit Testing Job performs the following steps:
1. **Reading Configuration**: It reads the column names and expected data types for validation from the `config.yaml` file. This allows for flexibility in adapting to different data sources and structures.
2. **Running Unit Tests**: Using the configuration parameters, the job executes unit tests to ensure data integrity. Specifically, it checks whether:
   - The number of columns matches the expected count.
   - The data types of columns match the expected data types.
3. **Logging Test Results**: The results of each unit test are logged, including the status (pass or fail) and any relevant reasons for failure.
4. **Storing Test Results**: The job creates two target tables or files:
   - One table/file stores the unit test cases' status, providing a detailed record of the validation process.
   - The other job updates the pass or fail status of the overall job in the `daily_load_status` table. This table is used to monitor the data pipeline's daily health and includes status information for both unit testing and data quality checks.

### Running Unit Testing Job
To execute the Unit Testing Job, follow these steps:
1. Ensure that the main Data Pipeline Job has successfully completed its tasks, including creating the database and the external Spark SQL table.
2. Run the Unit Testing Job using the following command:

   python UnitTesting.py


## Data Quality Job

The Data Quality Job (`DataQuality.py`) is a critical component of the data pipeline process that runs once the data load is complete for the current day. Its primary purpose is to ensure data quality by performing various data validation checks.

### Data Quality Validation Process

The Data Quality Job executes the following data validation processes:
1. **Reading Configuration**: It reads configuration parameters from the `config.yaml` file, including information about primary key columns, expected data count, and any other data quality checks.
2. **Primary Key Check**: The job validates that there are no duplicate primary key values in the dataset. If duplicates are found, it logs an error and records the failure.
3. **Data Count Validation**: It checks if the actual data count is not less than the expected data count. If the count is less, it indicates a potential data loss issue and logs an error.
4. **Logging Validation Results**: The results of each data quality check, including status (pass or fail) and reasons for failure, are logged for future reference.
5. **Storing Validation Results**: The Data Quality Job creates two types of files or tables:
   - One table stores the status of data quality checks, providing detailed records of the validation process.
   - Another table is used to update the `daily_load_status` table. If any data quality check fails, it marks the job as "failed" for today for this datapipeline, ensuring that data quality issues are immediately addressed.

### Running the Data Quality Job
To execute the Data Quality Job, follow these steps:

1. Ensure that the main Data Pipeline Job, including unit testing, has successfully completed its tasks.
2. Run the Data Quality Job using the following command:

   python DataQuality.py

