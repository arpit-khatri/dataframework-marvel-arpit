import pandas as pd
import requests
import os
import logging
import datetime
import yaml

# Function to initialize the logger with the date-appended log file name
def init_logger(log_path):
    os.makedirs(log_path, exist_ok=True)
    log_file_name = f"Monitoring_{datetime.datetime.now().strftime('%Y%m%d')}.log"
    logging.basicConfig(
        filename=os.path.join(log_path, log_file_name),
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def send_teams_notification(message, webhook_url, load_status_df):
    try:
        #response = requests.post(webhook_url, json=payload)
        #response.raise_for_status()
        print("Load Daily Status Table:")
        print(load_status_df.to_string(index=False))
        logging.info("Teams notification sent successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to send Teams notification: {str(e)}")

def check_and_send_notification(load_status_path, webhook_url):
    try:
        # Read the daily_load_status table
        load_status_df = pd.read_csv(load_status_path)

        # Check if any row has a "FAIL" status
        if "FAIL" in load_status_df['Status'].values:
            # Send a Teams notification with the table as an attachment
            message = "Please check Data Quality log for detailed failures. Check the attached table for details."
            send_teams_notification(message, webhook_url, load_status_df)
            print(f"Error Message:{message}")
            logging.info("Teams notification sent due to failures.")

        else:
            # Send a Teams notification without the table
            message = "Data Quality Job completed successfully without failures."
            send_teams_notification(message, webhook_url, load_status_df)
            logging.info("Teams notification sent for successful run of all jobs.")

    except Exception as e:
        logging.error(f"Error while checking and sending notification: {str(e)}")

if __name__ == '__main__':
    try:
        logging.info("Monitoring Job Started.")
        # Loading configuration from config.yamlto get daily_load_status table and Teams webhook URL
        with open("config.yaml", "r") as config_file:
            config = yaml.safe_load(config_file)
        monitoring_path = config["monitoring_path"]
        teams_webhook_url = config["teams_webhook_url"]
        load_status_table = config["load_status_table"]
        logging_path = config["log_path"]

         # Initialize logger
        init_logger(logging_path)

        # Check the daily_load_status and send Teams notification
        check_and_send_notification(f"{monitoring_path}/{load_status_table}.csv", teams_webhook_url)
        logging.info("Monitoring Job completed successfully.")
    except Exception as e:
        # Handling fatal errors
        error_message = str(e)
        logging.error(f"Monitoring Job failed: {error_message}")
