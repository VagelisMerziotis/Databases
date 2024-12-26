import pandas as pd
import requests
import datetime
import os

def log_error(message):
    """Log error messages with timestamp."""
    timestamp = datetime.datetime.now()
    formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S") + f".{timestamp.microsecond // 1000:03d}\n"
    log_file = 'event_log.txt'
    with open(log_file, 'a') as log:
        log.write(f"{formatted_timestamp}\t\t{message}")
    print(f"Error: {message}")  # Print error for immediate visibility in logs

def log_info(message):
    """Log info messages with timestamp."""
    timestamp = datetime.datetime.now()
    formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S") + f".{timestamp.microsecond // 1000:03d}\n"
    log_file = 'event_log.txt'
    with open(log_file, 'a') as log:
        log.write(f"{formatted_timestamp}\t\t{message}")
    print(f"Info: {message}")  # Print info for immediate visibility in logs

"""
This script is purposed to connec to the CoinGecko API and fetch the requested data every 2 minutes.
"""
try:
    # Log the current working directory to ensure we're in the expected place
    cwd = os.getcwd()
    log_info(f"Airflow Working Directory: {cwd}")

    # Make a simple request to CoinGecko for 5 cryptocurrency prices.
    response = requests.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": "bitcoin,ethereum,tether,ripple,solana", "vs_currencies": "usd"}
    )

    # Log the status code of the response
    log_info(f"API request made to CoinGecko. Status code: {response.status_code}")

    """
    Careful logging was implemented on this .py file in order to catch occuring problems in the execution of the subsequent .py file.
    """
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        
        # Process the data and create the dataframe
        crypto_data = {crypto: details['usd'] for crypto, details in data.items()}
        
        # Create the DataFrame and add timestamp
        data_df = pd.DataFrame([crypto_data])
        
        # Appropriately format timestamp 
        formatted_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + f".{datetime.datetime.now().microsecond // 1000:03d}"
        timestamp_df = pd.DataFrame({'timestamp': [formatted_timestamp]}, index=data_df.index)

        # Concatenate the timestamp with the cryptocurrency data
        final_df = pd.concat([timestamp_df, data_df], axis=1)

        # Ensure the path exists for saving a .csv snapshot of data
        save_dir = '/home/admin1997/Documents/portfolio/automation'
        crypto_file_path = os.path.join(save_dir, 'crypto.csv')

        # Log the file path for debugging
        log_info(f"Saving file to path: {crypto_file_path}")
        """
        During DAG testing several errors occured directly linked to bad pathing.
        For that reason we must ensure to use the cd command or the os library to change the  working directory appropriately.
        """
        # Attempt to save the dataframe to a CSV file
        try:
            final_df.to_csv(crypto_file_path, index=False)
            log_info(f"Successfully saved the data to {crypto_file_path}.")
        except Exception as e:
            log_error(f"Failed to save CSV file: {str(e)}")
            raise  # Reraise the error to halt further execution

        # Check if the file actually exists
        if os.path.exists(crypto_file_path):
            log_info(f"Confirmed that the file {crypto_file_path} exists.")
        else:
            log_error(f"File {crypto_file_path} does not exist after saving.")
            raise FileNotFoundError(f"{crypto_file_path} does not exist after saving.")

    else:
        # If the API request failed, log the failure
        log_error(f"API request failed with status code: {response.status_code}")

except requests.exceptions.RequestException as e:
    # Handle any issues with the request, such as network errors
    log_error(f"Request error: {str(e)}")

except Exception as e:
    # Catch any other exceptions (e.g., issues with data processing or file handling)
    log_error(f"An unexpected error occurred: {str(e)}")
