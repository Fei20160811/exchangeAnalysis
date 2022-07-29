#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul  6 15:33:19 2022

@author: ml
"""

from datetime import date, datetime, timedelta
import time
import requests 
import psycopg2
    
def str_to_num(s, c_type):
    #將字串s移除逗點與句點後轉為float/int
    #若非float 或 int則不處理，直接回傳
    if c_type not in ['float', 'int']:
        return s
    
    s = s.replace(',', '')
    try:
        if c_type == 'int':
            return int(s)
        else:
            return float(s)
    except:
        return -1
    
def GetConnStr(db_name):
    # connection string information
    host = "localhost"
    dbname = db_name
    user = "postgres"
    password = "123456"
    sslmode = "allow"
    
    return "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)

def crawl_price(date):
    #將datetime物件字串化為YYYYMMDD格式
    datestr = date.strftime('%Y%m%d')
    
    print('https://www.twse.com.tw/exchangeReport/MI_INDEX?'+'response=json&date='+ datestr +'&type=0099P')
    
    #從證交所網站上獲取指定日期的所有個股資訊
    resp = requests.get('https://www.twse.com.tw/exchangeReport/MI_INDEX?'
                       +'response=json&date='+ datestr +'&type=0099P')
    data = resp.json()
    
    #如果當天沒有資料
    if 'data1' not in data: 
        return None
    
    #欄位定義：
    #["證券代號"、"日期（新增）"、"成交股數"、"成交筆數"、"成交金額"、
    # "開盤價"、"最高價"、"最低價"、"收盤價"、"漲跌價差"
    # "最後揭示買價"、"最後揭示買量"、"最後揭示賣價"、"最後揭示賣量"、"本益比"]
    
    types = ['text', 'datetime', 'int', 'int', 'int',
             'float', 'float', 'float', 'float', 'float', 
             'float', 'int', 'float', 'int', 'float']
    prices = []
    
    for item in data['data1']:
        #忽略成交股數為0的資料列
        if item[2] == '0':
            continue
        #第二欄(證券名稱)及第１０欄(漲跌(+/-))不需要，故移除之
        filtered = item[:1] + item[2:9] + item[10:]
        #插入日期欄位到第2欄
        filtered = filtered[:1] + [datestr] + filtered[1:]
        
        prices.append([str_to_num(s, types[i]) for i, s in enumerate(filtered)])
        
    return prices

def bulk_insert(fname, bulk_data):    
    # Construct connection string
    conn = psycopg2.connect(GetConnStr(fname))
    print("Connection established")
    
    # Insert some data into the table
    cursor = conn.cursor()
    for d in bulk_data:
        values = ["'"+ str(e) + "'" for e in d]
        cmd = 'INSERT INTO exchange.daily_price VALUES({})'.format(','.join(values)) 
        cursor.execute(cmd)
    conn.commit()
    cursor.close()
    conn.close()

def update_db(date_from, date_to):
    print('更新資料：{} 到 {}'.format(date_from.strftime('%Y-%m-%d'), date_to.strftime('%Y-%m-%d')))
    current = date_from
    while current <= date_to:
        prices = crawl_price(current) 
       
        print(prices)
        if prices:
            bulk_insert('postgres', prices)
            print(current.strftime('%Y-%m-%d'), '...成功')
        else:
            print(current.strftime('%Y-%m-%d'), '...失敗（可能為假日）')
            
        current += timedelta(days=1)
        time.sleep(5) #放慢爬蟲速度
    
def get_date_range_from_db(fname):
    conn = psycopg2.connect(GetConnStr(fname))
    print("Connection established")
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM exchange.daily_price order by "TransDate" ASC LIMIT 1;')
    #print(cursor.mogrify(queryStr)) #列印出sql指令
    row = cursor.fetchone()
    print(row)
    
    if row == None: #第一次新增資料，起始日期設為昨日
        date_from = date.today() + timedelta(days=-1)
        date_to = date.today() + timedelta(days=-1)
    else:        
        date_from = row[1] #datatype:datetime.date
        cursor.execute('SELECT * FROM exchange.daily_price order by "TransDate" DESC LIMIT 1;')
        row = cursor.fetchone()
        print(row)
        date_to = row[1] #datatype:datetime.date
    cursor.close()
    conn.close()
    return date_from, date_to

#主程式流程
db_from, db_to = get_date_range_from_db('postgres')
print('資料庫日期：{} 到 {}'.format(db_from.strftime('%Y-%m-%d'), db_to.strftime('%Y-%m-%d')))

date_from = db_to + timedelta(days=1)
date_to = date.today() #datetime.today() v.s. date.today()

update_db(date_from, date_to)
