# -*- coding: utf-8 -*-
"""
Created on Mon Jan 14 19:41:49 2019

@author: kosuke
"""

import os
from datetime import datetime
from time import sleep
import ccxt
import json
import traceback


# save_dictをjson形式でfile_pathに保存する
def save_dict_as_json(file_path, save_dict):
    #    save_dir = os.path.dirname(file_path)
    #    if not os.path.exists(save_dir):
    #        os.makedirs(save_dir)
    save_text = json.dumps(save_dict, indent=True)
    save_file = open(file_path, 'w+')
    save_file.write(save_text)
    save_file.close()
    print('Save a dictionary to ' + file_path)


#if __name__ == 'main':

save_dir_name = 'orderbook'
if not os.path.exists(save_dir_name):
    os.makedirs(save_dir_name)

bitflyer = ccxt.bitflyer()
bitflyer.apiKey = '9KMDohNkN98E3R3PNxpR3A'
bitflyer.secret = 'FHeL+SZp6KgIAgCEnmIPJZIxYuYtjAHf42JZqBxPEWk='

while True:
    try:
        orderbook = bitflyer.fetch_order_book(
            'BTC/JPY', params={"product_code": "FX_BTC_JPY"})
        now = datetime.now()
        #orderbook['datetime'] = now
        orderbook['timestamp'] = now.strftime('%Y-%m-%d %H:%M:%S.%f')
        print('Got an orderbook at ' + orderbook['timestamp'])

        file_name = now.strftime('%Y%m%d%H%M%S.%f') + '.json'
        file_path = save_dir_name + '/' + file_name
        save_dict_as_json(file_path, orderbook)

        sleep(1)
    except Exception as e:
        with open('error.log', 'a') as f:
            f.write(str(datetime.now))
            f.write('\n')
            f.write(str(e))
            f.write(traceback.format_exc())
        sleep(60)
        pass
