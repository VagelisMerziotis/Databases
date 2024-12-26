import psycopg2
import datetime

def log_error(message):
    """Log error messages with timestamp."""
    timestamp = datetime.datetime.now()
    formatted_timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S") + f".{timestamp.microsecond // 1000:03d}\n"
    with open('event_log.txt', 'a') as log:
        log.write(f"{formatted_timestamp}\t\t{message}")


"""
This script is to be executed once in order to create our cryptobase.
A connection is made with python implementation.
A timestamp column followed by the prices of our selected cryptocurrencies.
"""
try:
    # Connect to the default 'postgres' database to check if 'cryptobase' exists
    conn = psycopg2.connect(
        dbname="postgres", 
        user="admin", 
        password="admin", 
        host="localhost", 
        port="5432"
    )

    # Enable autocommit for CREATE DATABASE command
    conn.set_session(autocommit=True)

    cursor = conn.cursor()

    # Check if 'cryptobase' database exists
    cursor.execute("""
        SELECT 1 FROM pg_database WHERE datname = 'cryptobase';
    """)
    exists = cursor.fetchone()

    if not exists:
        cursor.execute("CREATE DATABASE cryptobase;")
        log_error("Database 'cryptobase' did not exist and was created.")
    else:
        log_error("Database 'cryptobase' already exists.")

    # Close the connection to the 'postgres' database
    cursor.close()
    conn.close()

    # Now connect to the newly created or existing 'cryptobase' database
    conn = psycopg2.connect(
        dbname="cryptobase", 
        user="admin", 
        password="admin", 
        host="localhost", 
        port="5432"
    )

    cursor = conn.cursor()

    # Create the 'currency' table if it does not exist
    sql_create_table = """
        CREATE TABLE IF NOT EXISTS currency (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP(3),
            bitcoin INT,
            ethereum INT,
            ripple INT,
            solana INT,
            tether INT
        );
    """
    cursor.execute(sql_create_table)
    conn.commit()
    log_error("Table 'currency' checked/created successfully.")

    # Always close the connection to the database when the script is done
    cursor.close()
    conn.close()

except Exception as e:
    # Catch exceptions and log detailed error messages
    log_error(f"An error occurred: {str(e)}")
    log_error(f"Full error traceback: {str(e.__traceback__)}")

