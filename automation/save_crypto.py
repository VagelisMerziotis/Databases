import pandas as pd
import datetime
from sqlalchemy import create_engine

# Timestamp initiation for logging
timestamp = datetime.datetime.now()
formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S") + f".{timestamp.microsecond // 1000:03d}"

# Read data from the CSV file
data = pd.read_csv('crypto.csv')

# Database configuration
db_config = {
    'dbname': 'cryptobase',
    'user': 'admin',
    'password': 'admin',
    'host': 'localhost',
    'port': '5432',
}

"""
Originally the scipt failed in spotting the existance of crypto.csv. It turned out to be a problem in the local directory.
For that reason, we should consider local direcotry changes when running DAGs, especially when using bash operators and any local paths.
We can either use full paths for file destinations or change the local direcory before executing the code scripts.
"""
try:
    # Create SQLAlchemy connection engine
    engine = create_engine(f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")
    
    # Load data into the currency table in PostgreSQL
    data.to_sql('currency', con=engine, if_exists='append', index=False)    
    # Log success
    with open('event_log.txt', 'a') as log:
        log.write(f"{formatted_timestamp}\t\tData loaded successfully into the cryptobase database.\n")

except Exception as e:
    # Log error
    with open('event_log.txt', 'a') as log:
        log.write(f"{formatted_timestamp}\t\tAn error occurred while loading data: {e}\n")



