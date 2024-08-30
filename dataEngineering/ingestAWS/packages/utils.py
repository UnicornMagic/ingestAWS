import requests
import os
import pandas as pd 
import s3fs
from datetime import datetime

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
    
def load_to_s3(df: pd.DataFrame, bucket_name: str, file_name: str, aws_access_key_id: str, aws_secret_access_key: str, aws_session_token: str = None):
    """
    Write a DataFrame to an S3 bucket as a Parquet file.

    Parameters:
    - df: The DataFrame to write.
    - bucket_name: The name of the S3 bucket.
    - file_name: The name of the Parquet file to be saved (e.g., 'data.parquet').
    - aws_access_key_id: Your AWS access key ID.
    - aws_secret_access_key: Your AWS secret access key.
    - aws_session_token: (Optional) Your AWS session token, if using temporary credentials.

    Notes: This function is intended for incremental loads, so when the function runs, it will automatically dump
    the data that was ingested in a partition folder with the execution date. 
    """
    
    # Get the current date
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Ensure the file name ends with .parquet (had to do this because I could not see the extension in the S3 menu)
    if not file_name.endswith(".parquet"):
        file_name += ".parquet"

    # Create the S3 file path
    s3_file_path = f"s3://{bucket_name}/{current_date}/{file_name}"

    # Write the DataFrame to S3 as a Parquet file
    try:
        df.to_parquet(
            s3_file_path, 
            index=False, 
            storage_options={
                "key": aws_access_key_id,
                "secret": aws_secret_access_key,
                "token": aws_session_token
            }
        )
        print(f"Data successfully written to {s3_file_path}")
    except Exception as e:
        print(f"Failed to write data to S3: {e}")