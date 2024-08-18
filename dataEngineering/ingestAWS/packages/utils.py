import requests
import os
import pandas as pd 

api_key = os.getenv('AV_API_KEY')
date_format = "%Y-%m-%d"


def fetch_daily_stock(stock:str):
    # Call API with desired currency
    url = "https://www.alphavantage.co/query"
    params = {
    "function": "TIME_SERIES_DAILY",
    "symbol": stock,
    "datatype":"json",
    "outputsize":"compact",#compact/full
    "apikey": api_key
    }

    #Get and put respinse in Dictionary 
    response = requests.get(url, params=params, verify=False)
    data = response.json()

    #  Transform dict to dataframe with date as index
    df = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient='index')
    df.columns = ['open', 'high', 'low', 'close', 'volume']

    # Set columns to correct format 
    df.index = pd.to_datetime(df.index, format=date_format)
    df = df.apply(pd.to_numeric)
    
    return df.sort_index(ascending=True)

def fetch_daily_fx(currency):
    # Call API with desired currency
    url = "https://www.alphavantage.co/query"
    params = {
    "function": "FX_DAILY",
    "from_symbol": currency,
    "to_symbol": "USD",
    'datatype':'json',
    'outputsize':'compact',#compact/full
    "apikey": api_key
    }

    #Get and put respinse in Dictionary 
    response = requests.get(url, params=params, verify=False)
    data = response.json()

    #  Transform dict to dataframe with date as index
    df = pd.DataFrame.from_dict(data['Time Series FX (Daily)'], orient='index')
    df.columns = ['open', 'high', 'low', 'close']

    # Set columns to correct format 
    df.index = pd.to_datetime(df.index, format=date_format)
    df = df.apply(pd.to_numeric)
    
    return df.sort_index(ascending=True)

def fetch_weekly_fx(currency):
    # Call API with desired currency
    url = "https://www.alphavantage.co/query"
    params = {
    "function": "FX_WEEKLY",
    "from_symbol": currency,
    "to_symbol": "USD",
    'datatype':'json',
    'outputsize':'compact',#compact/full
    "apikey": api_key
    }

    #Get and put respinse in Dictionary 
    response = requests.get(url, params=params, verify=False)
    data = response.json()

    #  Transform dict to dataframe with date as index
    df = pd.DataFrame.from_dict(data['Time Series FX (Weekly)'], orient='index')
    df.columns = ['open', 'high', 'low', 'close']

    # Set columns to correct format 
    df.index = pd.to_datetime(df.index, format=date_format)
    df = df.apply(pd.to_numeric)
    
    return df.sort_index(ascending=True)

def fetch_crude_oil():
    # Call API with desired currency
    url = "https://www.alphavantage.co/query"
    params = {
    "function": "WTI",
    "interval": "monthly",
    'datatype':'json',
    # 'outputsize':'full',#compact/full
    "apikey": api_key
    }
    response = requests.get(url, params=params, verify=False)
    data = response.json()


    df = pd.DataFrame(data['data'])

    # Transform Data to dataframe 
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df['value'] = pd.to_numeric(df['value'])    
    
    return df.sort_index(ascending=True)

def fetch_crude_oil_brent():
    # Call API with desired currency
    url = "https://www.alphavantage.co/query"
    params = {
    "function": "BRENT",
    "interval": "monthly",
    'datatype':'json',
    # 'outputsize':'full',#compact/full
    "apikey": api_key
    }

    response = requests.get(url, params=params, verify=False)
    data = response.json()


    df = pd.DataFrame(data['data'])

    # Transform Data to dataframe 
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    df['value'] = pd.to_numeric(df['value'])  
    
    return df.sort_index(ascending=True)

def get_row_by_date(df: pd.DataFrame, date: str) -> pd.Series:
    # Ensure the date is in the correct format
    try:
        date = pd.to_datetime(date)
    except ValueError:
        raise ValueError("The date must be in the format 'YYYY-MM-DD'")
    
    # Query the row by the specified date
    if date in df.index:
        return df.loc[date]
    else:
        raise KeyError(f"No data found for the date {date}")