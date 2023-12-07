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


# # 24시간 분량

# In[2]:


df= pd.read_csv("C:/Users/user/4-3/crypto/group_project2/csv/2023-11-10-exchange-market-orderbook.csv")


# In[3]:


groups = df.groupby('timestamp')
keys = groups.groups.keys()


# # 함수 정의

# In[4]:


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


# In[5]:


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

# In[6]:


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

