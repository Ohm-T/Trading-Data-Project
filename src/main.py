import os
from src.topic_management import topic_management
from src.ingest_data import*
from src.consumer import consumer
if __name__=='_main__':
    #os.popen('sh ./start_kafka')
    list_tickers=['goog','aapl','baba','bnp.pa','ing', 'nvs']

    # Create topics if needed
    topic_management(list_tickers)
    # Load data
    data = load_tickers(list_tickers)
    # Send data to Topics
    ingest_data(data)
    # Get the data from kafka consumer and update river model

    # Send the classification to Topics prediction
    
