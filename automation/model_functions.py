import psycopg2

import pandas as pd
import numpy as np
import os
from datetime import datetime , timedelta

from sklearn.preprocessing import MinMaxScaler
from keras.models import load_model
from keras.models import Sequential
from keras.layers import LSTM, Dense

"""
    Our Postgres base already exists from the 1st part of the project.
    We can make multiple extentions but this time we wills tick to LSTM creation and train automation on daily data.
    
    This script also has the purpose to create the LSTM model and train it for the 1st time. It must be run as a standalone script 
    once before deployment of the rest of the pipeline.
"""

"""
    The fetch_all_data function is aimed to fetch all teh previous data upon first time deployment
    in order to tran the LSTM initially.
"""
def fetch_all_data():
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        dbname="cryptobase",
        user="admin",
        password="admin",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    # SQL query to get all cryptocurrency data
    query = """
        SELECT timestamp, bitcoin
        FROM currency
        ORDER BY timestamp;
    """
    cursor.execute(query)
    rows = cursor.fetchall()

    # Create DataFrame
    df = pd.DataFrame(rows, columns=['timestamp', 'bitcoin'])
    conn.close()
    return df


"""
    The fetch_daily_data is aimed at retrieving the data from the past 24 hours. The DAG will be deployed daily meaning it will
    be eventually trained on all available data.
    Furthermore, the system will not be heavily loaded if training happens daily and in smaller batches compaired to scarce, big scale trainings.
"""
def fetch_daily_data():
    conn = psycopg2.connect(
        dbname="cryptobase",
        user="admin",
        password="admin",
        host="localhost",
        port="5432"
    )
    cursor = conn.cursor()

    # Query to get data for the last 24 hours
    yesterday = datetime.now() - timedelta(days=1)
    query = """
        SELECT timestamp, bitcoin
        FROM currency
        WHERE timestamp > %s
        ORDER BY timestamp;
    """
    cursor.execute(query, (yesterday,))
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['timestamp', 'bitcoin'])
    conn.close()
    
    return df

"""
    Preprocessing of data after retreival. Fixing data types and splitting the timestamp into seperate days, hours and months.
    Year could also be part of the data.
    Subsequently we scale the data with MinMaxScaler, an important process for AI training. 
    Lastly, discrete steps are taken in the dataset (every 30 records -> 2 minutes x 30 =1 hour)
    This way we can reduce redundant data.
        
"""
def preprocess_data(df):
    # Ensure 'timestamp' is in datetime format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    
    # Extract time-based features (optional)
    df['hour'] = df.index.hour
    df['day_of_week'] = df.index.dayofweek
    df['month'] = df.index.month
    
    # Sort the data by timestamp
    df = df.sort_index()

    # Feature scaling (for price)
    scaler = MinMaxScaler(feature_range=(0, 1))
    scaled_data = scaler.fit_transform(df[['bitcoin']].values)
    
    # Create the dataset with time steps (e.g., 60 previous time steps)
    def create_dataset(data, time_step=30):
        X, y = [], []
        for i in range(time_step, len(data)):
            X.append(data[i-time_step:i, 0])  # X is a sequence of 60 time steps
            y.append(data[i, 0])  # y is the next value to predict
        return np.array(X), np.array(y)

    # Prepare dataset
    X, y = create_dataset(scaled_data)
    
    # Reshape X for LSTM input: [samples, time steps, features]
    X = X.reshape(X.shape[0], X.shape[1], 1)
    
    return X, y, scaler

"""
    Initial creation of a simple LSTM model.        
"""
def create_lstm_model(input_shape):
    model = Sequential()
    model.add(LSTM(50, return_sequences=True, input_shape=input_shape))
    model.add(LSTM(50))
    model.add(Dense(1))  # Output layer for predicting the price (single value)
    model.compile(optimizer='adam', loss='mean_squared_error')
    return model

"""
The train_and_save_model function trains the model and saves it after each training.
If the model exists then it will be loaded.
If we wanted to avoid the if statement then we would have to modify the code so as to call another function to load it and reduce the calling of the create_model
function to once accross the whole workflow.

"""
def train_and_save_model(X_train, y_train):
    model_path = 'crypto_lstm_model.keras'  # Path to the model file

    # Check if the model already exists
    if os.path.exists(model_path):
        print("Model exists, loading the model...")
        model = load_model(model_path)  # Load the existing model
        model.fit(X_train, y_train)
    else:
        print("Model doesn't exist, creating and training a new model...")
        model = create_lstm_model(X_train.shape[1:])  # Create a new model
        model.fit(X_train, y_train, epochs=50, batch_size=32)  # Train the new model

    # Save the trained model (or re-save if it was loaded)
    model.save(model_path)
    print(f"Model trained and saved as {model_path}!")
    

"""
    The main function binds everything together
"""
def main():
    df = fetch_all_data()  # Step 1: Retrieve data
    X, y, scaler = preprocess_data(df)  # Step 2: Preprocess data
    train_and_save_model(X, y)  # Step 3 & 4: Train and save the model
    



if __name__ == "__main__":
    main()
    
