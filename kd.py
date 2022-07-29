#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 28 17:53:41 2022

@author: ml
"""
import psycopg2
from decimal import *

def GetConnStr(db_name):
    # connection string information
    host = "localhost"
    dbname = db_name
    user = "postgres"
    password = "123456"
    sslmode = "allow"
    
    return "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)


def get_data(fname, stock_id, period):
    conn = psycopg2.connect(GetConnStr(fname))
    cursor = conn.cursor()
    cmd = 'SELECT "TransDate", "ClosingPrice" FROM exchange.daily_price WHERE "StockId"=\'{:s}\' ORDER BY "TransDate" DESC LIMIT {:d};'.format(stock_id, period)
    #print(cursor.mogrify(cmd)) #列印出sql指令
    cursor.execute(cmd)
    rows = cursor.fetchall()
    rows.reverse()
    conn.close()
    
    return rows

def calc_rsv(prices): #採用9、3、3方式計算KD值
    window = prices[:8] #前8天股價
    #因不足9天，前8天最高點、最低點、及RSV值皆為0; 第8天的K值＝D值＝50
    #k_values、k_values轉型為Decimal，解決float與Decimal無法相加相乘的錯誤
    highest = [0]*8
    lowest = [0]*8
    rsv_values = [0]*8
    k_values = [0]*7 + [Decimal(50)]
    d_values = [0]*7 + [Decimal(50)]
    
    #從第9天開始計算RSV及KD值
    for i , p in enumerate(prices[8:]):
        window.append(p)
        window = window[len(window)-9:] #計算範圍為最近9天
        high = max(window)
        low = min(window)
        rsv = 100 * ((p-low) / (high-low))
        
        k = (Decimal((1/3)) * rsv) + (Decimal((2/3)) * k_values[i-1])
        d = (Decimal((1/3)) * k) + (Decimal((2/3)) * d_values[i-1])
        
        highest.append(high)
        lowest.append(low)
        rsv_values.append(rsv)
        k_values.append(k)
        d_values.append(d)
    return k_values, d_values

def get_buy_signal(k_values, d_values):
    buy = [0] * 8 #前8天沒有資料故沒有買進訊號
    
    for i in range(8, len(k_values)):
        #策略：KD黃金交叉(前一天 k < d 且今天 k > d)且在低檔(30)
        if(k_values[i-1] < d_values[i-1] and k_values[i] > d_values[i] and k_values[i] < 30):
            #print(k_values[i])
            buy.append(1)
        else:
            buy.append(0)
    return buy
    

db_name = 'postgres'
price_data = get_data(db_name, '0050', 90)
dates = [d[0] for d in price_data]
prices = [d[1] for d in price_data]
print('起始日期：{}(收盤價：{}), 結束日期：{}(收盤價：{})({}天)))'.format(dates[0], prices[0], dates[-1], prices[-1], len(dates)))
k, d = calc_rsv(prices)
buy = get_buy_signal(k, d)
print('本金10萬元，期間有 {} 次買進訊號，一次投入1萬元'.format(sum(buy)))
profit = [10]
ratios = [1] + [prices[i] / prices[i-1] for i in range(1, len(prices))]
#因為買進訊號是根據當天盤後價格計算，隔日才能真正加碼
#故將買進訊號往右平移一天當作當天加碼1萬元
buy = [0] + buy[:-1]
for b,r in zip(buy[1:], ratios[1:]):
    profit.append(profit[-1] * r +b)

print('回測結果：{}'.format(profit[-1]))
interest_rate = 0.011 * (30/365)
total = 10 + sum(buy)
print('{}萬元定存結果(利率1.1%):{}'.format(total, total*(1+interest_rate)))