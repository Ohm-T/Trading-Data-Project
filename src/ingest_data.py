from kafka import KafkaProducer
import yfinance as yf
import json
import time

def load_tickers(list_tickers=['goog','aapl','baba','bnp.pa','ing', 'nvs']):
    """Load the data for multiple companies in list_tickers
    return the dataframe"""
    tickers = yf.Tickers(list_tickers)
    df = tickers.download(period='2y', group_by='ticker')
    return df.reset_index(drop=False)



def ingest_data(df):
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], value_serializer=lambda v: json.dumps(v).encode())
    for i, row in df.iterrows():
        date = row[df.columns[0]]
        for company, stock in df.columns[1:]:
            values = row[company].to_dict()
            values['Date'] = str(date)
            producer.send(topic=company.lower(), key=b'Tesla Stock Update', value=values)
            print(f"Producing to {company.lower()}")
            producer.flush()
        if i%300==0:
            print(f"300 days sent to the topics")

            

