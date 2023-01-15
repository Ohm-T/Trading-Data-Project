from kafka import KafkaProducer
import yfinance as yf
from sklearn.impute import KNNImputer
from collections import OrderedDict
import json
import pandas as pd
import numpy as np
import sys

def load_tickers(list_tickers=['goog','aapl','baba','bnp.pa','ing', 'nvs'], horizon=100):
    """Load the data for multiple companies in list_tickers
    return the dataframe"""
    tickers = yf.Tickers(list_tickers)
    df = tickers.download(period='2y', group_by='ticker')
    # take last 3 months of data as labels
    df = df.reset_index(drop=False)
    dict_companies = {'train':dict(), 'test':dict()}
    for company in list_tickers:
        df_company = df[company.upper()]
        imputer = KNNImputer(n_neighbors=1)
        df_company = pd.DataFrame(imputer.fit_transform(df_company), columns=df_company.columns)
        df_company['Date'] = df['Date']
        df_company , df_labels = df_company.iloc[:-horizon], df_company.iloc[-horizon:]

        dict_companies['train'][company.upper()] = df_company
        dict_companies['test'][company.upper()] = df_labels

    return dict_companies

def producer_data(list_dict, list_tickers=['goog','aapl','baba','bnp.pa','ing', 'nvs']):
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], value_serializer=lambda v: json.dumps(v).encode())
    for company in list_tickers:
        topic_name = company.upper()
        res = list_dict[topic_name].to_dict('index')
        for i in range(len(res)):
            res[i]['Date'] = str(res[i]['Date'])
            producer.send(company, res[i])
            producer.flush()
            if i==400:
                print(f"{i} rows sent to {topic_name}")
            
        if company == list_tickers[-1]:
            print("End")
            break
    sys.exit(1)




            

