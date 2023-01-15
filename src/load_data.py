import yfinance as yf
from sklearn.impute import KNNImputer
import pandas as pd


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