#!/usr/bin/env python
# coding: utf-8


import time
import requests
import pandas as pd
import datetime
import os

# 시작 시간을 기록합니다.
start_time = datetime.datetime.now()

while True:
    cycle_start_time = datetime.datetime.now()  # 사이클 시작 시간 기록
    try:
        # 현재 시간을 확인합니다.
        current_time = datetime.datetime.now()
        # 시작 시간으로부터 얼마나 많은 시간이 지났는지 계산합니다.
        elapsed_time = current_time - start_time
        if elapsed_time.total_seconds() >= 216000:  
            break    
        
        
        # 데이터를 가져와서 처리하는 부분
        book = {}
        response = requests.get ('https://api.bithumb.com/public/orderbook/BTC_KRW/?count=5')
        book = response.json()

        data = book['data']

        bids = (pd.DataFrame(data['bids'])).apply(pd.to_numeric,errors='ignore')
        bids.sort_values('price', ascending=False, inplace=True)
        bids = bids.reset_index(); del bids['index']
        bids['type'] = 0
    
        asks = (pd.DataFrame(data['asks'])).apply(pd.to_numeric,errors='ignore')
        asks.sort_values('price', ascending=True, inplace=True)
        asks['type'] = 1 

        print (bids)
        print ("\n")
        print (asks)

        df = bids.append(asks)
        
        timestamp = datetime.datetime.now()
        df['timestamp'] = timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        df['quantity'] = df['quantity'].round(decimals=4)
        
        
        fn = "./2023-11-09-bithumb-btc-orderbook.csv"  
            
            
        #헤더를 첫 행에만 입력합니다. 
        should_write_header = os.path.exists(fn)        
        if should_write_header == False:
            df.to_csv(fn, index=False, header=True, mode = 'a')
        else:
            df.to_csv(fn, index=False, header=False, mode = 'a')

    except Exception as e:
        print(f"An error occurred: {e}")
        # 오류가 발생하면 반복을 중지합니다.
        continue
        
        
    # 사이클 종료 시간을 기록하고 1초 간격을 유지하기 위해 대기합니다.
    cycle_end_time = datetime.datetime.now()
    elapsed_time_in_cycle = (cycle_end_time - cycle_start_time).total_seconds()
    time_to_wait = max(1 - elapsed_time_in_cycle, 0)  # 1초 미만이 남았다면 대기
    time.sleep(time_to_wait)        

