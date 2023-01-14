from kafka import KafkaProducer
import yfinance as yf
from sklearn.impute import KNNImputer
import json
import pandas as pd
import numpy as np

def load_tickers(list_tickers=['goog','aapl','baba','bnp.pa','ing', 'nvs']):
    """Load the data for multiple companies in list_tickers
    return the dataframe"""
    tickers = yf.Tickers(list_tickers)
    df = tickers.download(period='2y', group_by='ticker')
    df = df.reset_index(drop=False)
    numpy_company = np.zeros((520, 7))

    for company in list_tickers:
        df_company = df[company.upper()]
        imputer = KNNImputer(n_neighbors=1)
        numpy_company = np.hstack((numpy_company, imputer.fit_transform(df_company)))
    numpy_company = numpy_company[:, 7:]
    df_companies = pd.DataFrame(numpy_company, columns=df.columns[1:])
    df_companies['Date'] = df['Date']
    return df_companies

def ingest_data(df):
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], value_serializer=lambda v: json.dumps(v).encode())
    for i, row in df.iterrows():
        date = row[df.columns[-1]]
        for company, stock in df.columns[:-1]:
            values = row[company].to_dict()
            values['Date'] = str(date)
            producer.send(topic=company.lower(), value=values)
            producer.flush()
        if i%1000==0:
            print(f"1000 rows sent at all companies")


            

