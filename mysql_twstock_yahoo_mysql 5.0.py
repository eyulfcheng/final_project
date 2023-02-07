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
    return datetime.datetime(2021,int(month),int(day))

def timestamp_trans(timestamp):
    string = str(timestamp)
    return  string

def rest():
    random_second = round(random.random() * 3 + 10, 2)#一分鐘內不能超過五次連線
    print('Let me sleep for...',random_second,'seconds')
    time.sleep(random_second)

def mysql_catch_code():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    # MySQL的使用者：root, 密碼:root, 埠：3306,資料庫：ptt_stock
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/ptt_stock')
    # 查詢語句，選出unit表中的所有資料
    sql = 'select * from ptt_stock.unit'
    df = pd.read_sql_query(sql, engine)
    fliter = df['stock_code'] != ' '  
    fliter2 = df['class'] == '標的'
    # fliter = (df['stock_code'] != ' ' and df['class'] == '標的')
    dd = df[fliter & fliter2]
    de = dd.groupby('stock_code').size().sort_values(ascending = False)
    return de, df

    
def to_mysql(df, code_name):
    myconn = mysql.connector.connect(host= "localhost",user = "root", password = "root")
    mycursor = myconn.cursor()    
    mycursor.execute('USE ptt_stock;')
    # pblu('DROP TABLE IF EXISTS stock_'+ code_name)
    mycursor.execute('DROP TABLE IF EXISTS stock_'+ code_name)
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/ptt_stock')    
    df.to_sql( 'stock_'+code_name, engine, index= True)
    pblu( code_name + ' table 創建成功\n')
    # mycursor.commit()
    mycursor.close()
    myconn.close()    

def reptile_twstock(de, top_num = 15): 
    
    de_p = de[de > top_num]
    list_code = list(de_p.index) 
    pblu('list_code:')
    print(list_code,'\n')
    de_p.plot(kind = 'bar')
    plt.show()
    pblu('list load succesed\n')
    
    list_price_list = []
    start = datetime.datetime(2021, 1, 1)
    end   = datetime.datetime(2021 ,3,28)
    #######################################################################
    list_price_list = []
    # seq = list(range(1,32))
    list_index = list(range(1,len(list_code)+1))
    list_day = list(range(1,32))
    df_stock_code_detailed = pd.DataFrame(columns=['type', 'code','name','    ISIN','    start','market','    group','    CFI', '    times'], index = list_index)
    df_stock_code_price = pd.DataFrame(columns = list_day, index = de_p.index )
    for i, code in enumerate(list_code):
        # time.sleep(5)
        if code in twstock.twse:  
            list_info = list(twstock.codes[code])
            list_info.append(de[i])
            df_stock_code_detailed.iloc[i] = list_info
            print(list_info,'\n31天內股價變動讀取中...')
            pblu(str(code) + df_stock_code_detailed.iloc[i][2]+ ' 讀取成功')
        else :
            pred(str(code) + ' 讀取失敗') 
            rest()              
            if code in twstock.twse:  
                list_info = list(twstock.codes[code])
                list_info.append(de[i])
                df_stock_code_detailed.iloc[i] = list_info
                print(list_info)
                rest()
                pblu(str(code) + df_stock_code_detailed.iloc[i][2]+ ' 再次讀取成功')
            else:
                pred(str(code) + ' 再次讀取失敗\n')
                continue
        df_yahoo = data.DataReader((code +'.TW'),'yahoo',start,end).round(2)
        df_yahoo = df_yahoo.reset_index()
        # print()
        
        list_month = []
        list_day = []
        for i in range(len(df_yahoo)):            

            list_month.append( timestamp_trans(df_yahoo.iloc[i][0])[5:7].replace('0',''))
            list_day.append(timestamp_trans(df_yahoo.iloc[i][0])[8:10])

        df_yahoo['month'] = list_month
        df_yahoo['day'] = list_day
        
        # print(df_yahoo)
        to_mysql(df_yahoo, code)
        rest()
    print(df_stock_code_detailed, 'df_stock_code_detailed1')
    to_mysql(df_stock_code_detailed, 'code' )
    # print(df_stock_code_detailed, 'df_stock_code_detailed2')
    return df_stock_code_detailed


de = mysql_catch_code()[0]
df = mysql_catch_code()[1]
df_scd = reptile_twstock(de,10)

de_p = de[de > 5]
list_code = list(df_scd['code'])
# list_code = list_code.remove('8436')
print(list_code , 'list_code')



