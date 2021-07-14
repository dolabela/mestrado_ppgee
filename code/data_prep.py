import requests 
import pandas as pd 
from io import StringIO
import time 
import os 
import pickle
import numpy as np
import string 
import nltk
nltk.download('punkt')
from nltk.tokenize import word_tokenize
from pandas_datareader import data as pdr
from datetime import datetime
from datetime import date
import yfinance as yf
yf.pdr_override()

def gen_dates(index, time_is, time_os):
    t=0
    t_aux=t
    size = len(index) 
    dates = []
    while t + time_is + 1 + time_os < size:
        init_is = index[t]
        end_is = index[t + time_is]
        init_os = index[t + time_is + 1]
        end_os = index[t + time_is + time_os]
        row = [init_is, end_is, init_os, end_os]
        dates.append(row)
        t+=time_os
        if (t + time_is + 1 + time_os >= size) and (t + time_is + 1 <= size  ):
            init_is = index[t]
            end_is = index[t + time_is]
            init_os = index[t + time_is + 1]
            end_os = index[size -1]
            row = [init_is, end_is, init_os, end_os]
            dates.append(row)
    return dates


def get_stock_prices_yf (ticker, field = 'Adj Close'):
    try:
        today = date.today()
        start_date='2019-01-01'
        df = pdr.get_data_yahoo(ticker, start=start_date, end=today)
        df = df['Adj Close']
        print("Concluded", ticker)
    except:
        print(ticker, 'not found')
    finally:
        return df

def get_multiple_stock_prices_yf (ticker_list, key = '', field = 'adjusted_close', function = 'TIME_SERIES_DAILY_ADJUSTED', outputsize = 'full'):
    prices_dict = {}
    for t in ticker_list:
        prices_dict[t] = get_stock_prices_yf (t, field = 'Adj Close')
    return prices_dict

def get_stock_prices_av (ticker, key, field = 'adjusted_close', function = 'TIME_SERIES_DAILY_ADJUSTED', outputsize = 'full'):
    link = 'https://www.alphavantage.co/query?function={}&symbol={}&apikey={}&outputsize={}&datatype=csv'.format(function, ticker,key,outputsize)
    print("Getting data from", ticker)
    try:
        r = requests.get(link)
        df = pd.read_csv(StringIO(r.text))
        df.index = df.timestamp
        df = df[field].sort_index()
        print("Concluded", ticker)
        time.sleep(13)

    except:
        print(ticker, 'not found')
    finally:
        return df

def get_multiple_stock_prices_av (ticker_list, key, field = 'adjusted_close', function = 'TIME_SERIES_DAILY_ADJUSTED', outputsize = 'full'):
    prices_dict = {}
    for t in ticker_list:
        prices_dict[t] = get_stock_prices_av(ticker = t,
                                        key = key,
                                       field = field,
                                       function = function,
                                       outputsize = outputsize)
    return prices_dict

def get_bloomberg_data_from_drive ():
    columns = ['created_at','text', 'extended_tweet', 'retweet_count']
    df = pd.DataFrame(columns = columns )
    for i in range(90):
      rows = pd.read_csv('data/raw/bloomberg30/ {}.csv'.format(i))
      rows = rows[columns]
      df = df.append(rows)
      df['fixed_text'] = df.apply(get_text, axis = 1)
      df['tokenized_text'] = [word_tokenize(i) for i in df['fixed_text']]
    return df

def get_text(row):
    # print(row['extended_tweet'])
    if row['extended_tweet'] is np.nan:
        return row.text
    else:
      # print(row['extended_tweet'][1] == "'")
        if row['extended_tweet'][1] == "'":
            return row['extended_tweet'].split("'")[3]
        else:
            return row['extended_tweet'].split('"')[3]


def preprocess_text (df):
    df['fixed_text'] = df.apply(get_text, axis = 1)
    df['tokenized_text'] = [word_tokenize(i) for i in df['fixed_text']]
    return df   

def obtain_polarities(df, negative, positive, stopwords):

    def analyse_polarity_positive (row):
        score = 0
        for i in row['tokenized_text']:
            token = i.lower()
            if token in positive:
                score += 1
        return (len(row['tokenized_text']) - score)/len(row['tokenized_text']) 

    def analyse_polarity_negative (row):
        score = 0
        for i in row['tokenized_text']:
            token = i.lower()
            if token in negative:
                score += 1
        return (score)/len(row['tokenized_text']) 

    def analyse_polarity (row):
        positive_score = 0
        negative_score = 0
        score = 0
        for i in row['tokenized_text']:
            token = i.lower()
            if token in positive:
                positive_score += 1
                score += 1
            elif token in negative:
                negative_score += 1
                score += 1
            elif token not in stopwords:
                flag = 0
                for t in token:
                    if t in string.punctuation:
                        flag = 1
                        break
                if flag == 0:
                    score += 1
        if score > 0:            
            return (positive_score - negative_score)/score
        else:
            return 0
    def corona_counter (row):
        if ('orona' in row['text']):
            return 1
        else:
            return 0 


    # df['positive_score'] = df.apply(analyse_polarity_positive, axis = 1)
    df['negative_score'] = df.apply(analyse_polarity_negative, axis = 1)
    df['polarity'] = df.apply(analyse_polarity, axis = 1)  
    df['corona'] = df.apply(corona_counter, axis = 1)
    # break 
    return df 


def get_fixed_date(df, date_grid):

    # def from_string_to_datetime (row):
    #     return datetime.strptime(row['created_at'].replace(' +0000', ''), "%a %b %d %H:%M:%S %Y" ).replace(hour=0, minute=0, second=0, microsecond=0)

    def get_fixed_date (row):
        date_complete = datetime.strptime(row['created_at'].replace(' +0000', ''), "%a %b %d %H:%M:%S %Y" )
        date = date_complete.replace(hour=0, minute=0, second=0, microsecond=0)
        hour = date_complete.hour 
        if (date in date_grid) and hour <= 16:
            return date 
        else:
            return date_grid[date_grid > date][0]


        
    
    df['datetime'] = df.apply(get_fixed_date, axis = 1) 
    return df 

