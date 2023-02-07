
################################### import ####################################
import os
import re
import time
import datetime
import requests
import random
import twstock
import pymysql
import pandas as pd
import mysql.connector
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
from pandas_datareader import data 
from tkinter import ttk
import tkinter as tk
import tkinter.font as tf

###############################################################################

################################# basic define ################################
def pred(string_input):                                        # print 紅色
    string_cal = '\033[1;31m' + string_input  + '\033[0m'
    print(string_cal)
def pblu(string_input):                                        # print 藍色
    string_cal = '\033[1;34m' + string_input  + '\033[0m'
    print(string_cal)

def find(inp_to_find, string):                                 # 尋找(re)，沒找到返還空格
    re_find = re.findall(inp_to_find, string)
    if re_find:return re_find
    else:return [' ']    
    
def ptt_date_trans(date =' 3/01'):                             # 字串轉時間
    re_find = re.findall('(.*?)/', date)
    if re_find:month = re_find[0]
    else:month = 1        
    re_find = re.findall('/(\d{2})', date)
    if re_find:day = re_find[0]
    else:day = 1
    return datetime.datetime(2021,int(month),int(day))

def timestamp_trans(timestamp):                                # 時間格式轉字串
    return str(timestamp)

def rest():                                                    # 一分鐘內不能超過五次連線
    random_second = round(random.random() * 3 + 10, 2)
    print('Let me sleep for...',random_second,'seconds')
    time.sleep(random_second)    
###############################################################################

################################# ptt to mysql ################################
def create_mysql():
    myconn = mysql.connector.connect(host= "localhost",user = "root", password = "root")
    mycursor = myconn.cursor()    
    mycursor.execute('DROP DATABASE IF EXISTS ptt_stock')
    mycursor.execute('CREATE DATABASE ptt_stock')
    pblu('succesed connect mysql and create database')
    mycursor.close()    
    mycursor = myconn.cursor()
    
    list_sql_com = [
        'USE ptt_stock;',
        'DROP TABLE IF EXISTS unit',
        '''CREATE TABLE unit  (
        `id`        VARCHAR(50) PRIMARY KEY,
        `Re`        VARCHAR(4) ,
        `class`     VARCHAR(255) ,
        `title`     VARCHAR(255) ,
        `month`     VARCHAR(4) ,
        `day`       VARCHAR(4) ,
        `like`      int(255) ,
        `html`      VARCHAR(255) ,
        `stock_code`VARCHAR(4)
        );''']
        
    for unit_sql_com in list_sql_com:
        mycursor.execute(unit_sql_com)
    pblu('succesed create table')
    # mycursor.commit()
    mycursor.close()
    myconn.close()

def mysql_insert(myconn, str_inp):
    mycursor = myconn.cursor()
    mycursor.execute("USE ptt_stock;")    
    mycursor.execute("INSERT INTO unit VALUES(" + str_inp +")")
    myconn.commit()
    mycursor.close()
    
def reptile_ptt_a_page( url, limit_date, n ):
    print(url + '讀取中...')
    HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36'}
    myconn = mysql.connector.connect(host= "localhost",user = "root", password = "root")
    webpage = requests.get( url, headers = HEADERS, cookies={'over18':'1'})
    # print(webpage)
    soup = BeautifulSoup(webpage.text,'html.parser')        
    bs4_name = soup.find_all('div', class_='r-ent')
    
    if n == 0 :
        bs4_name = bs4_name[:-4:]
    
    for i in bs4_name :            
        string = str(i)
        unit = find('<a href=(.+?)</a>\n</div>', string)[0]
        if unit == 'None':
            continue        
        date = find('<div class="date">(.*?)</div>', string)[0]
        if date == limit_date:
            print('不抓'+ str(limit_date) +'(含)之前的文')
            break
        mont = find('(\d*?)\/\d{2}', date)[0]
        days = find('\d*?\/(\d{2})', date)[0]
        like = find('class=.*">(.*?)<', string)[0]
        if like == '':like =0
        elif like =='爆':like = 200
        elif 'X' in like :like = 0
        elif int(like):like = int(like)
        else:like = 0                
        html = 'https://www.ptt.cc' + find('"(/bbs/Stock/M.*?)"',unit)[0]
        unid = str(n).zfill(10)
        unre = find('[A-Z][a-z]:\s',unit)[0]
        clas = find('\[(.*)\]',unit)[0]
        titl = find('(?<=]).+',unit)[0]
        if titl == 'None':
            continue
        titl = titl.replace('"',' ').replace('\'',' ')
        code = find('\d{4}',titl)[0]
        str_ins = '"'+ unid +'","'+ unre+'","'+ clas +'","'+ titl +'","'+ mont +'","'+ days +'","'+ str(like) +'","'+ html +'","'+ code +'"'
        mysql_insert(myconn, str_ins)
        n += 1 
            
        end_date = date      
        bs4_previous_page = soup.find_all('a', class_='btn wide')            
        string_pre_pag = str(bs4_previous_page[1])
        url_pre_pag = 'https://www.ptt.cc' + find('<a class="btn wide" href="(.*?)">‹ 上頁', string_pre_pag)[0] 
        n_sum = n
    return url_pre_pag, limit_date, n_sum, end_date 
    
def reptile_ptt_pages( n_limit = 100000, limit_date = '12/31'):
    create_mysql()
    reptile = True
    page = 0
    pre_pag =  ['https://www.ptt.cc/bbs/Stock/index.html', limit_date, 0]
    while reptile:
        pre_pag = reptile_ptt_a_page(url = pre_pag[0], limit_date = pre_pag[1], n = pre_pag[2])
        page += 1
        str_progress = '第 ' + str(page) + ' 頁' +'，共 ' + str(pre_pag[2]) + ' 筆資料' + str(pre_pag[3])
        pblu(str_progress)
        if pre_pag[2] > n_limit:
            pred('n 到達設定值' + str(n_limit))
            reptile = False
        elif pre_pag[3] == limit_date:
            pred('日期 到達設定值' + str(limit_date))
            reptile = False
    return '共 ' + str(page) + ' 頁網頁' +'，共 ' + str(pre_pag[2]) + ' 筆資料' + str(pre_pag[3])
# reptile_ptt_pages()    
###############################################################################################

########################### twstock and yahoo ( code from mysql ) ############################# 
def mysql_catch_code():
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/ptt_stock')
    sql = 'select * from ptt_stock.unit'
    df = pd.read_sql_query(sql, engine)
    fliter = df['stock_code'] != ' '  
    fliter2 = df['class'] == '標的'
    dd = df[fliter & fliter2]
    de = dd.groupby('stock_code').size().sort_values(ascending = False)
    return de, df

def to_mysql(df, code_name):
    myconn = mysql.connector.connect(host= "localhost",user = "root", password = "root")
    mycursor = myconn.cursor()    
    mycursor.execute('USE ptt_stock;')
    mycursor.execute('DROP TABLE IF EXISTS stock_'+ code_name)
    engine = create_engine('mysql+pymysql://root:root@localhost:3306/ptt_stock')    
    df.to_sql( 'stock_'+code_name, engine, index= True)
    pblu( code_name + ' table 創建成功\n')
    mycursor.close()
    myconn.close()
    
def reptile_twstock(de, top_num = 10):     
    de_p = de[de > top_num]
    list_code = list(de_p.index) 
    pblu('list_code:')
    print(list_code,'\n')
    de_p.plot(kind = 'bar')
    plt.savefig('bar.png',bbox_inches='tight')
    plt.show()
    pblu('list load succesed\n')
    
    list_price_list = []
    start = datetime.datetime(2021, 1, 2)
    end   = datetime.datetime(2021 ,3,28)

    list_index = list(range(1,len(list_code)+1))
    list_day = list(range(1,32))
    df_stock_code_detailed = pd.DataFrame(columns=['type', 'code','name','    ISIN','    start','market','    group','    CFI', '    times'], index = list_index)
    df_stock_code_price = pd.DataFrame(columns = list_day, index = de_p.index )
    for i, code in enumerate(list_code):
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
        
        list_month = []
        list_day = []
        for i in range(len(df_yahoo)):
            list_month.append( timestamp_trans(df_yahoo.iloc[i][0])[5:7].replace('0',''))
            list_day.append(timestamp_trans(df_yahoo.iloc[i][0])[8:10])

        df_yahoo['month'] = list_month
        df_yahoo['day'] = list_day        
        to_mysql(df_yahoo, code)
        rest()
    print(df_stock_code_detailed, 'df_stock_code_detailed1')
    to_mysql(df_stock_code_detailed, 'code' )
    return df_stock_code_detailed

# de = mysql_catch_code()[0]
# df = mysql_catch_code()[1]
# df_scd = reptile_twstock(de,5)

# de_p = de[de > 5]
# list_code = list(df_scd['code'])
# print(list_code , 'list_code')
###############################################################################################

#################################### draw line and plot #######################################
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
    dd = df[fliter]
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
        plt.savefig('photo'+code+'_bar.png',bbox_inches='tight')
        plt.show()
        
        sns.heatmap(df_yahoo[['ptt_Count','Like','Volume','Close']].corr(), annot=True, cmap='Wistia')
        plt.title(code)
        plt.savefig('photo'+code+'_heatmap.png',bbox_inches='tight')
        plt.show()
###############################################################################################

############################################# GUI #############################################
class tk_show():
    def __init__(self): # 創立板面區 ##左中右
        #######################################################################################################################################
        self.win = tk.Tk()
        self.win.geometry('800x480')
        self.win.config(bg = 'lightgray')
        self.win.title('PTT股板討論與股價關係圖')
        self.win.iconbitmap('buss.ico')
        self.fm_text = tk.Frame(self.win, bg='lightgray', width = 30, height = 40)
        self.fm_text.pack(side = tk.RIGHT)
        
        # self.fm_photo = tk.Frame(self.win, bg='lightgray', width = 30, height = 60)
        # self.fm_photo.pack(side = tk.LEFT, fill = tk.Y)
        # self.lab_photo_top = tk.Label( self.fm_photo, bg = 'skyblue', text= '上顯示區塊', width = 40 ,height = 13)
        # self.lab_photo_top.pack(side = tk.TOP, padx = 10, pady = 10)
        # self.lab_photo_down = tk.Label( self.fm_photo, bg = 'lightgray', width = 42 ,height = 1)
        # self.lab_photo_down.pack(side = tk.TOP, padx = 10)
        # self.lab_photo_down = tk.Label( self.fm_photo, bg = 'pink', text= '下顯示區塊', width = 40 ,height = 13)
        # self.lab_photo_down.pack(side = tk.TOP, padx = 10)
        # self.lab_photo_down = tk.Label( self.fm_photo, bg = 'lightgray', width = 42 ,height = 1)
        # self.lab_photo_down.pack(side = tk.TOP, padx = 10, pady = 5)
        
        self.fm_control = tk.Frame(self.win, bg='lightgray', width = 40, height = 60)
        self.fm_control.pack(side = tk.LEFT)
        
        self.fm_control_content = tk.Frame(self.fm_control, bg='gray', width = 300, height = 460)
        self.fm_control_content.pack(side = tk.TOP)    
        self.fm_control_content.propagate(0)
        
        
        self.text_string = '文字顯示區...\n'
        self.ft = tf.Font(family='微软雅黑',size=8)
        self.scrollbar1 = tk.Scrollbar(self.fm_text) #滚动条是要放在窗口上，其参数为窗口对象
        self.muilt_entry = tk.Text(self.fm_text, width = 80, height = 40, yscrollcommand = self.scrollbar1.set,font =self.ft)    
        self.muilt_entry.insert(tk.END, self.text_string)
        # muilt_entry.insert('end','123') #末尾插入
        self.scrollbar1.config(command = self.muilt_entry.yview)
        self.scrollbar1.pack(side = tk.RIGHT, fill = tk.Y)
        self.muilt_entry.pack(side = tk.RIGHT, padx=10, pady=10)  

        ##############################################################################################################################################
        # 控制區
        ##############################################################################################################################################
        def com_date_confirm(): # 確認按鈕
            if self.day_inp:
                if self.month_inp:
                    self.date_inp = self.month_inp+'/'+self.day_inp
                    self.muilt_entry.insert('end','設定日期為')
                    self.muilt_entry.insert('end', self.date_inp)
                    self.muilt_entry.insert('end', '執行中')
                    self.progress = reptile_ptt_pages( n_limit = 100000, limit_date = self.date_inp)
                    
        self.list_month = [' 1',' 2',' 3',' 4',' 5',' 6',' 7',' 8',' 9','10','11','12']
        self.list_day_m = ['01','02','03','04','05','06','07','08','09','10','11','12',
                         '13','14','15','16','17','18','19','20','21','22','23','24',
                         '25','26','27','28','29','30','31'] 
        self.fm_control_date  =  tk.Frame    (self.fm_control_content, bg = 'lightgray', width = 300, height = 420)
        self.btn_control_date =  tk.Button   (self.fm_control_date, width =  5, height = 1, text = '確認', command = com_date_confirm)
        self.lab_month        =  tk.Label    (self.fm_control_date, text = '月份', width =  5, height = 2 , bg = 'lightgray' , fg = 'black'  )    
        self.opt_month        = ttk.Combobox (self.fm_control_date, values = self.list_month)
        self.lab_day          =  tk.Label    (self.fm_control_date, text = '日期', width =  5, height = 2 , bg = 'lightgray' , fg = 'black'  )
        self.opt_day          = ttk.Combobox (self.fm_control_date, values = self.list_day_m  )
        def callback_month(event):            
            self.opt_day.pack_forget()
            if  self.opt_month.get() == ' 2': 
                self.list_day = self.list_day_m[:28]               
            elif self.opt_month.get() in [' 4',' 6',' 9','11']: 
                self.list_day = self.list_day_m[:30]
            else:
                self.list_day = self.list_day_m[:31]
            self.month_inp = self.opt_month.get()
            self.opt_day   = ttk.Combobox (self.fm_control_date, values = self.list_day  )
            self.opt_day.config(width = 4, font = ('Helvetica', 8))
            self.lab_day  .pack(side = tk.LEFT , padx=2)
            self.opt_day  .pack(side = tk.LEFT , padx=2)                  
            self.muilt_entry.insert('end',' {}月'.format(self.month_inp))
            def callback_day(event): 
                self.  day_inp = self.opt_day  .get()
                self.muilt_entry.insert('end',' {}日\n'.format(self.  day_inp))
            self.opt_day  .bind("<<ComboboxSelected>>", callback_day  )
        self.opt_month.bind("<<ComboboxSelected>>", callback_month)


        self.opt_month.config(width = 4, font = ('Helvetica', 8))
        self.opt_month.current(0)
        self.fm_control_date.pack( side = tk.TOP  , fill = tk.X)
        self.btn_control_date.pack(side = tk.RIGHT, padx=2)
        self.lab_month.pack(       side = tk.LEFT , padx=2)
        self.opt_month.pack(       side = tk.LEFT , padx=2)
      ##################################################################################################################################################
        def com_df_do():
            self.list_de_df = mysql_catch_code()
            self.de = self.list_de_df[0]
            self.df_scd = reptile_twstock(self.de, 10)            
            self.de_p = self.de[self.de > 10]
            self.list_code = list(self.df_scd['code'])
            print(self.list_code , 'list_code')
            self.muilt_entry.insert('end',self.df_scd)
            self.opt_photo   = ttk.Combobox (self.fm_control_date, values = self.list_code  )
            self.opt_photo.config(width = 4, font = ('Helvetica', 8))
            self.lab_photo  .pack(side = tk.LEFT , padx=2)
            
        def com_df_show():
            self.stock_code = mysql_catch_df(df = 'stock_code')
            self.muilt_entry.insert('end', self.stock_code[[ 'code','name','    group', '    times']])
            # self.newWindow = tk.Toplevel(self.win)
            # self.top_photo = tk.Label(self.newWindow, width =  500, height = 350 , bg = 'white' )
            # self.top_photo.pack(side = 'top')
            # self.down_photo = tk.Label(self.newWindow, width =  500, height = 350 , bg = 'white' )
            # self.down_photo.pack(side = 'bottom')
            
            # pic_val = tk.PhotoImage(file = '123.png')
            # label_picture = tk.Label( win, image = pic_val )
            # label_picture.pack(side = 'right')
    
        self.fm_control_twstock  =  tk.Frame    (self.fm_control_content, bg = 'lightgray', width = 300, height = 420)
        self.btn_df_do           =  tk.Button   (self.fm_control_twstock, width =  5, height = 1, text = '執行', command = com_df_do  )
        self.btn_df_show         =  tk.Button   (self.fm_control_twstock, width =  5, height = 1, text = '顯示', command = com_df_show)   
        # self.opt_month           =  tk.Button (self.fm_control_twstock, values = self.list_month)
        
        # self.opt_day             =  tk.Button (self.fm_control_twstock, values = self.list_day_m  )

        self.fm_control_twstock.pack(side = tk.TOP , fill = tk.X)
        self.btn_df_do  .pack(side = tk.RIGHT , padx=2)
        self.btn_df_show.pack(side = tk.RIGHT , padx=2)
        # def createNewWindow():
        #     self.newWindow = tk.Toplevel(self.win)
        #     self.labelExample = tk.Button(self.newWindow, text = "New Window button")
        #     self.label.pack()
        # self.buttonExample = tk.Button(self.fm_control_twstock, text="Create new window", command = createNewWindow)
        # self.buttonExample.pack(side = tk.TOP,fill = 'x')
    
    
    
    
    
    
        self.win.mainloop()

test = tk_show()





































       
    
