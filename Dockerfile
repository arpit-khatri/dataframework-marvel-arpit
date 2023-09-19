# Use an official PySpark image as the base image
FROM jupyter/pyspark-notebook:latest

# Set the working directory inside the container
WORKDIR /app/code

# Copy your application code and configuration files to the container
COPY ./code /app/code
COPY ./source /app/source
COPY ./target /app/target
COPY ./logs /app/logs
COPY ./monitoring /app/monitoring
COPY ./requirements.txt /app/code

# Install any additional Python dependencies (if needed)
RUN pip install -r requirements.txt

# Define the entry point for running your PySpark applications in sequence
CMD ["bash", "-c", "python /app/code/DataPipeline.py && python /app/code/Analytics.py && python /app/code/UnitTesting.py && python /app/code/DataQuality.py && python /app/code/Monitoring.py"]