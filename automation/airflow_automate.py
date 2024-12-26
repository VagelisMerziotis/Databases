from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'admin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=0.1),
}

# Define the DAG
with DAG(
    'crypto_pipeline',    
    default_args=default_args,
    description='A pipeline to run crypto scripts for currency value acquisition with API key and python implementation, using BashOperator',
    schedule_interval="*/2 * * * *",  # Runs every 2 minutes for better data resolution.
    start_date=datetime(2024, 12, 24),
    catchup=False,
) as dag:
    
    """
    Each seperate .py script is executed to do collect prices from an API and then store the information in the cryptobase.
    An important note to make here is the following:
    When a task is initiated in Airflow, it begins in its own shell. This most likely results in a different default directory than the one we store the 
    executable files. For taht reason we must include an explicit command to change directories appropriately before executing any script.
    """
    
    # Task 1: Fetch crypto prices with bash operator.
    fetch_crypto_prices = BashOperator(
        task_id='fetch_crypto_prices',
        bash_command="""
        echo "Activating conda environment..."
        source /home/admin1997/miniconda3/bin/activate tensorflow && \
        echo "Running crypto script..." && \
        cd /home/admin1997/Documents/portfolio/automation && \ 
        python get_crypto_prices.py && \
        conda deactivate 
        """
    )
    
    # Task 2: Save crypto data in cryptobase.
    save_crypto = BashOperator(
        task_id='save_crypto',
        bash_command="""
        
        echo "Activating postgresql environment..."
        source /home/admin1997/miniconda3/bin/activate postgresql && \
        echo "Running save crypto script..." && \
        cd /home/admin1997/Documents/portfolio/automation && \
        python save_crypto.py && \
        conda deactivate 
        """
    )


    fetch_crypto_prices >> save_crypto
