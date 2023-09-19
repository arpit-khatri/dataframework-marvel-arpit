## Data Framework

The Data Framework is a collection of five distinct jobs, each serving a unique purpose within the data pipeline and data quality assurance process. These jobs collaborate to ensure data integrity, quality, and timely issue resolution. Here's an overview of each job:

## 1- Data Pipeline Job

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

Step to run DataPipeline job:

         python DataPipeline.py


## 2- Data Analytics Job
The Data Analytics Job is designed to enable users to perform data analytics using Spark SQL on a specified DataFrame. 
Idea of this Analytics jobs is to show the analytics caplibility to the users that they can perform on this data framework solution.

Step to run Anlaytics job:
      
         python Analytics.py   

## 3- Unit Testing Job

The Unit Testing Job (`UnitTesting.py`) is an essential part of the data pipeline process. It runs once the table is created for today's load. The primary purpose of this job is to validate the quality of the data by performing unit tests.

### Unit Testing Process
The Unit Testing Job performs the following steps:
1. **Running Unit Tests**: Using the configuration parameters, the job executes unit tests to ensure data integrity. Specifically, it checks whether:
   - The number of columns matches the expected count.
   - The data types of columns match the expected data types.
2. **Logging Test Results**: The results of each unit test are logged, including the status (pass or fail) and any relevant reasons for failure.
3. **Storing Test Results**: The job creates two target tables or files:
   - One table/file stores the unit test cases' status, providing a detailed record of the validation process.
   - The other job updates the pass or fail status of the overall job in the `daily_load_status` table. This table is used to monitor the data pipeline's daily health and includes status information for both unit testing and data quality checks.

### Running Unit Testing Job
To execute the Unit Testing Job, follow these steps:
1. Ensure that the main Data Pipeline Job has successfully completed its tasks, including creating the database and the external Spark SQL table.
2. Run the Unit Testing Job using the following command:

       python UnitTesting.py


## 4- Data Quality Job

The Data Quality Job (`DataQuality.py`) is a critical component of the data pipeline process that runs once the data load is complete for the current day. Its primary purpose is to ensure data quality by performing various data validation checks.

### Data Quality Validation Process
The Data Quality Job executes the following data validation processes:
1. **Primary Key Check**: The job validates that there are no duplicate primary key values in the dataset. If duplicates are found, it logs an error and records the failure.
2. **Data Count Validation**: It checks if the actual data count is not less than the expected data count. If the count is less, it indicates a potential data loss issue and logs an error.
3. **Logging Validation Results**: The results of each data quality check, including status (pass or fail) and reasons for failure, are logged for future reference.
4. **Storing Validation Results**: The Data Quality Job creates two types of files or tables:
   - One table stores the status of data quality checks, providing detailed records of the validation process.
   - Another table is used to update the `daily_load_status` table. If any data quality check fails, it marks the job as "failed" in the `daily_load_status` table for today for this datapipeline, ensuring that data quality issues are immediately addressed.

### Running the Data Quality Job
To execute the Data Quality Job, follow these steps:

1. Ensure that the main Data Pipeline Job, including unit testing, has successfully completed its tasks.
2. Run the Data Quality Job using the following command:

         python DataQuality.py

## 5- Monitoring Job
The Monitoring Job is essential for keeping the Data Ops team informed about any issues with data quality and unit testing. 
Data Quality Assurance and Timely Issue Resolution by doing the status reporting with the help Automation on daily load staus job.


Steps Performed:

Read Configuration: The job reads the config.yaml file to obtain configuration parameters, including the path to the daily_load_status table and the Teams webhook URL.
Check Unit Testing and Data Quality Status: It checks the status of unit testing and data quality results in the daily_load_status table.

Send Teams Notifications:
If any data quality test fails (indicated by "FAIL" status in the table), a notification is sent.
If all tests pass successfully, a notification is sent to indicate successful load completion for today;s job.

Step to run Monitoring job:

      python Monitoring.py  