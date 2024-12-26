from model_functions import train_and_save_model, preprocess_data, fetch_daily_data


def retrain():
    data = fetch_daily_data()  # Retrieve data for the last 24 hours
    X, y, scaler = preprocess_data(data)  # Preprocess the data
    train_and_save_model(X, y)  # Train and save the model

retrain()