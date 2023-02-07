
import mysql.connector
import requests
import os
import re
import time
import datetime
from bs4 import BeautifulSoup

def pred(string_input):
    string_cal = '\033[1;31m' + string_input  + '\033[0m'
    print(string_cal)
def pblu(string_input):
    string_cal = '\033[1;34m' + string_input  + '\033[0m'
    print(string_cal)

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
# create_mysql()    
#######################################################################################
def find(inp_to_find, string):
    re_find = re.findall(inp_to_find, string)
    if re_find:
        return re_find
    else:
        return [' ']
    
# def ptt_date_trans(date =' 3/01'):
#     re_find = re.findall('(.*?)/', date)
#     if re_find:
#         month = re_find[0]
#     else:
#         month = '00'
        
#     re_find = re.findall('/(.*?)', date)
#     if re_find:
#         day = re_find[0]
#     else:
#         day = '00'
   
def mysql_insert(myconn, str_inp):
    mycursor = myconn.cursor()
    mycursor.execute("USE ptt_stock;")    
    mycursor.execute("INSERT INTO unit VALUES(" + str_inp +")")
    myconn.commit()
    mycursor.close()


##########################################################################################
def reptile_ptt_a_page( url, limit_date, n ):

    # print('url =',url,'limit_date=',limit_date,'n=',n)
    print(url + '讀取中...')
    HEADERS = {'user-agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36'}
    myconn = mysql.connector.connect(host= "localhost",user = "root", password = "root")
    mainloop = True
    while mainloop:
        webpage = requests.get( url, headers = HEADERS, cookies={'over18':'1'})
        # print(webpage)
        soup = BeautifulSoup(webpage.text,'html.parser')        
        bs4_name = soup.find_all('div', class_='r-ent')
        # print('bs4',bs4_name)
        # print('bs4 succesed')
        
        if n == 0 :
            bs4_name = bs4_name[:-4:]
        
        for i in bs4_name :
            
            string = str(i)
            # print(string)
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
            # unid = find('(\d{10})',unit)[0]
            unid = str(n).zfill(10)
            unre = find('[A-Z][a-z]:\s',unit)[0]
            clas = find('\[(.*)\]',unit)[0]
            titl = find('(?<=]).+',unit)[0]
            if titl == 'None':
                continue
            titl = titl.replace('"',' ').replace('\'',' ')
            code = find('\d{4}',titl)[0]
            # print(mont+days)
            # print(unit, date, like, html, unit, clas, titl)
            str_ins = '"'+ unid +'","'+ unre+'","'+ clas +'","'+ titl +'","'+ mont +'","'+ days +'","'+ str(like) +'","'+ html +'","'+ code +'"'
            # print("INSERT INTO unit VALUES(" + str_ins +")")
            mysql_insert(myconn, str_ins)
            n += 1 
            # pred('succesed a unit')
            
        end_date = date      
        bs4_previous_page = soup.find_all('a', class_='btn wide')            
        string_pre_pag = str(bs4_previous_page[1])
        url_pre_pag = 'https://www.ptt.cc' + find('<a class="btn wide" href="(.*?)">‹ 上頁', string_pre_pag)[0] 
        n_sum = n
        # print('url =',url,'limit_date=',limit_date,'n=',n_sum)
    
        # time.sleep(0.3)
        mainloop = False
    return url_pre_pag, limit_date, n_sum, end_date 
# reptile_ptt_a_page()




def reptile_ptt_pages( n_limit = 100000, limit_date = '12/31'):

    create_mysql()
    reptile = True
    page = 0
    pre_pag =  ['https://www.ptt.cc/bbs/Stock/index.html', limit_date, 0]
    while reptile:
        pre_pag = reptile_ptt_a_page(url = pre_pag[0], limit_date = pre_pag[1], n = pre_pag[2])
        page += 1
        pblu('第 ' + str(page) + ' 頁' +'，共 ' + str(pre_pag[2]) + ' 筆資料' + str(pre_pag[3]) )
        if pre_pag[2] > n_limit:
            pred('n 到達設定值' + str(n_limit))
            reptile = False
        elif pre_pag[3] == limit_date:
            pred('日期 到達設定值' + str(limit_date))
            reptile = False
    
reptile_ptt_pages()    
        
            
        
        
    
    
    





































































































