# -*- coding: utf-8 -*-
"""
Created on Thu Jan 17 19:53:09 2019

@author: kosuke
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import os


def load_orderbook(file_path):
    with open(file_path, 'r') as f:
        json_data = json.loads(f.read())
        
    bids = pd.DataFrame(json_data['bids'], columns=['price', 'size'])
    asks = pd.DataFrame(json_data['asks'], columns=['price', 'size'])
    time = datetime.strptime(json_data['timestamp'], '%Y-%m-%d %H:%M:%S.%f')

    return bids, asks, time    


def plot_orderbook(bids, asks, time):
    fig = plt.figure(figsize=(6,4), dpi=300)
    ax = fig.add_subplot(1,1,1)
    ax.set_title('bitflyer orderbook ' + str(time))
    ax.set_xlabel('price [JPY]')
    ax.set_ylabel('size [BTC]')
    ax.set_xlim([380000, 381000])
    ax.set_ylim([0, 100])
    ax.scatter(bids['price'], bids['size'], s=0.5, c='blue', label='bids')
    ax.scatter(asks['price'], asks['size'], s=0.5, c='red', label='asks')
    ax.legend()
    return fig    


def list_json_files(dir_path):
    import glob
    return glob.glob(dir_path + '/*.json')
    

base_dir_path = 'C:/home/project/bitflyer_orderbook_logger/orderbook_20190116/'
json_files = list_json_files(base_dir_path)

for json_file_path in json_files:
    bids, asks, time = load_orderbook(json_file_path)
    fig = plot_orderbook(bids, asks, time)
    fig.savefig(json_file_path + '.png')
    print(str(time))


#asks.describe()
#bids.describe()
