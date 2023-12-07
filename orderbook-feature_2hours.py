#!/usr/bin/env python
# coding: utf-8

# # Group Project: Phase 1

# ### AI와 암호화폐이야기

#  

# GitHub: https://github.com/nahyun81/NHSE-AIproject

# Group12 : 홍나현, 김성은

#  

# In[1]:


import pandas as pd


# In[2]:


df_project1= pd.read_csv("C:/Users/user/4-3/crypto/group_project2/csv/2023-11-10-exchange-market-orderbook.csv")


# # csv 파일에서 2시간만 새로운 csv 파일로 저장

# In[3]:


#'timestamp' 열을 날짜 및 시간 형식으로 변환하는 과정을 수행
df_project1['timestamp'] = pd.to_datetime(df_project1['timestamp'])

#'timestamp' 열에서 날짜 부분만 추출하여 그 고유한 값들을 unique_dates에 저장
unique_dates = df_project1['timestamp'].dt.date.unique()

# unique_dates에 저장된 각 날짜에 대해 반복문을 실행
for date in unique_dates:
    # date와 일치하는 날짜의 데이터만 선택하여 df_date라는 새로운 데이터 프레임에 저장
    df_date = df_project1[df_project1['timestamp'].dt.date == date]
    
    # 오전 2시 이전만 선택하여 df_date_2라는 새로운 데이터 프레임에 저장
    df_date_2 = df_date[df_date['timestamp'].dt.hour < 2]
    
    # 2023-11-10_2.csv로 저장
    df_date_2.to_csv(f'{date}_2.csv', index=False)


# # 2시간 데이터 csv 변수 저장

# In[4]:


df= pd.read_csv("C:/Users/user/4-3/crypto/group_project2/2023-11-10_2.csv")


# In[5]:


groups = df.groupby('timestamp')
keys = groups.groups.keys()


# # 함수 정의

# In[6]:


def cal_mid_price (gr_bid_level, gr_ask_level):

    level = 5

    if len(gr_bid_level) > 0 and len(gr_ask_level) > 0:
        bid_top_price = gr_bid_level.iloc[0].price
        bid_top_level_qty = gr_bid_level.iloc[0].quantity
        ask_top_price = gr_ask_level.iloc[0].price
        ask_top_level_qty = gr_ask_level.iloc[0].quantity
        mid_price = (bid_top_price + ask_top_price) * 0.5
        return (mid_price)

    else:
        print ('Error: serious cal_mid_price')
        return (-1)


# In[7]:


def live_cal_book_i_v1(param, gr_bid_level, gr_ask_level, diff, mid):
    
    mid_price = mid

    ratio = param[0]; level = param[1]; interval = param[2]

    quant_v_bid = gr_bid_level.quantity ** ratio
    price_v_bid = gr_bid_level.price * quant_v_bid

    quant_v_ask = gr_ask_level.quantity ** ratio
    price_v_ask = gr_ask_level.price * quant_v_ask
        
    askQty = quant_v_ask.sum()
    bidPx = price_v_bid.sum()
    bidQty = quant_v_bid.sum()
    askPx = price_v_ask.sum()
    bid_ask_spread = interval
        
    book_price = 0  # because of warning, divisible by 0
    if bidQty > 0 and askQty > 0:
        book_price = (((askQty * bidPx) / bidQty) + ((bidQty * askPx) / askQty)) / (bidQty + askQty)
        
    indicator_value = (book_price - mid_price) / bid_ask_spread
    
    return indicator_value


# # csv 저장

# In[8]:


timestamps = []
mid_prices = []
book_imbalances = []

for i in keys:
    gr_o = groups.get_group(i)
    
    # 'bid'와 'ask' 레벨의 데이터 프레임을 분리
    gr_bid_level = gr_o[(gr_o.type == 0)]
    gr_ask_level = gr_o[(gr_o.type == 1)]    
    
    # 중간 가격 계산 및 추가
    mid_price = cal_mid_price(gr_bid_level, gr_ask_level)
    mid_prices.append(mid_price)
    
    # 책 잔액 계산 및 추가
    book_imbalance = live_cal_book_i_v1((0.2,5,1), gr_bid_level, gr_ask_level, 'NaN', mid_price)
    book_imbalances.append(book_imbalance)
    
    # 타임스탬프 추가
    timestamps.append(i)


# 합친 데이터 프레임 만들기
new_df = pd.DataFrame({
    'timestamp': timestamps,
    'mid_price': mid_prices,
    'book-imbalance': book_imbalances
})

new_df.to_csv('2023-11-10-exchange-market-feature.csv', index=False)

