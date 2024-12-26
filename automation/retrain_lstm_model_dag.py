from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os, sys

# We append to the local directory to the direcotry with the model_functions.py to make sure it is reachable.
sys.path.append('/home/admin1997/Documents/portfolio/automation')  

# # Change the path appropriately to set the working directory to where all the scripts are
# os.chdir('/home/admin1997/Documents/portfolio/automation')

# Default arguments for the DAG
default_args = {
    'owner': 'admin',
    'retries': 5,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2024, 12, 27),
}

# Define the DAG
dag = DAG(
    'crypto_lstm_retrain',
    default_args=default_args,
    description='A DAG to retrain the LSTM model daily',
    schedule_interval='@daily',  # Runs once per day
    catchup=False
)

# Define the task to activate Conda environment, run the script, and then deactivate the environment
retrain_task = BashOperator(
    task_id='retrain_lstm_model',
    bash_command="""
        source /opt/conda/etc/profile.d/conda.sh && \
        conda activate tensorflow && \
        python retrain_lstm.py && \
        conda deactivate
    """,
    dag=dag
)

"""
The way this task works is by defining a function that calls other functions to load the model, collect the daily data, 
and retrain it, and subsequently set the calling of that function as an Airflow task.
"""

retrain_task  # Only one task in this DAG
