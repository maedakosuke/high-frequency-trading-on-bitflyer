# -*- coding: utf-8 -*-
"""
Created on Sun Feb 10 12:49:54 2019

@author: kosuke
"""

BUY = 1
SELL = -1
ITAYOSE = 0

SERVER_STATUS = ['NORMAL', 'BUSY', 'VERY BUSY']

# secrets
# Label: exclude_in_and_out_money
# bitFlyer Lightning API
API_KEY_BF = "9KMDohNkN98E3R3PNxpR3A"
API_SECRET_BF = "FHeL+SZp6KgIAgCEnmIPJZIxYuYtjAHf42JZqBxPEWk="
PRODUCT = 'FX_BTC_JPY'

def side_as_int(text):
    if text == 'BUY':
        return BUY
    elif text == 'SELL':
        return SELL
    else:
        return ITAYOSE
