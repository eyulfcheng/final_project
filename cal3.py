import mysql.connector
import time
import re
import random
import twstock
import pymysql
import datetime
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from pandas_datareader import data 

def pred(string_input):
    string_cal = '\033[1;31m' + string_input  + '\033[0m'
    print(string_cal)
def pblu(string_input):
    string_cal = '\033[1;34m' + string_input  + '\033[0m'
    print(string_cal)
    
    
def ptt_date_trans(date =' 3/01'):
    re_find = re.findall('(.*?)/', date)
    if re_find:month = re_find[0]
    else:month = 1        
    re_find = re.findall('/(\d{2})', date)
    if re_find:day = re_find[0]
    else:day = 1
    return datetime.date(2021,int(month),int(day))


def rest():
    random_second = round(random.random() * 3 + 10, 2)#一分鐘內不能超過五次連線
    print('Let me sleep for...',random_second,'seconds')
    time.sleep(random_second)
    

def mysql_catch_df(df = 'stock_code'):
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/ptt_stock')
    sql = 'select * from ptt_stock.'+ df
    dfm = pd.read_sql_query(sql, engine)
    return dfm


def mysql_catch_price(code):
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/ptt_stock')
    sql = 'select * from ptt_stock.stock_'+ code
    df = pd.read_sql_query(sql, engine)    
    return df

def normalization(df):
    de = df.rolling(5).mean().apply(lambda x : (x-np.min(x))/(np.max(x)-np.min(x))).round(2)
    return de

def mysql_catch_code2(code): # unit
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/ptt_stock')
    sql = 'select * from ptt_stock.unit'
    df = pd.read_sql_query(sql, engine)
    fliter = df['stock_code'] == code  
    # fliter2 = df['class'] == '標的'
    dd = df[fliter]
    # de = dd.groupby('stock_code').size().sort_values(ascending = False)
    return  dd


def catch_cal (list_code,dd):
    
    list_line_like = []
    list_line_count = []
    list_line_close = []
    list_line_volume = []
    for i,code in enumerate(list_code):
        
        df_yahoo = mysql_catch_price(code)
        df_yahoo = df_yahoo[['Close','Volume','month','day']]

        df_yahoo['ptt_Count'] = 0
        df_yahoo['Like'] = 0
        # print(df_yahoo)
        df_unit_t = mysql_catch_code2(code)
        # print(df_unit_t)
        list_mon = []
        list_day = []

        for i in range(len(df_unit_t)):
            lik = df_unit_t.iloc[i]['like']
            mon = df_unit_t.iloc[i]['month']
            day = df_unit_t.iloc[i]['day']
            mask1 = df_yahoo['month'] == mon
            mask2 = df_yahoo['day'] == day         
            df_yahoo.update(df_yahoo[(mask1 & mask2)]['ptt_Count'].apply(lambda x : x+1))
            df_yahoo.update(df_yahoo[(mask1 & mask2)]['Like'].apply(lambda x : int(x + lik)))
        # print(df_yahoo)
        df_yahoo = normalization(df_yahoo)
        close = df_yahoo['Close']
        line_c, = plt.plot(list(range(1,len(close)+1)), list(close))
        list_line_close.append(line_c)#list(range(1,len(close)))
        
        volume = df_yahoo['Volume']
        line_v, = plt.plot(list(range(1,len(volume)+1)), list(volume))
        list_line_volume.append(line_v)
        
        like = df_yahoo['Like']
        line_l, = plt.plot(list(range(1,len(like)+1)), list(like))
        list_line_like.append(line_l)
        
        count = df_yahoo['ptt_Count']
        line_c, = plt.plot(list(range(1,len(count)+1)), list(count))
        list_line_count.append(line_c)
        
        plt.title(code)
        plt.legend( labels = ['Close','Volume','Like','Count'], loc = 'best')
        plt.show()
        
        sns.heatmap(df_yahoo[['ptt_Count','Like','Volume','Close']].corr(), annot=True, cmap='Wistia')
        plt.title(code)
        plt.show()

        # print(code,df.corr().loc['close','volume'].round(2))
    # plt.legend(handles = list_line_close, labels = list_code, loc = 'best')
    # plt.show()
    # 
    # plt.show()


    # sns.heatmap(df.corr(), annot=True, cmap='coolwarm')
    # plt.show()

# list_code = ['2330', '2317', '2609', '2603', '1227', '3481', '2610', '2344', '2340'] 

list_code = list(filter(None,mysql_catch_df('stock_code')['code']))
print(list_code)
for i, code in enumerate(list_code) :
    dd = mysql_catch_code2(code)
    # print(dd['like'],dd['day'])
catch_cal (list_code,dd)