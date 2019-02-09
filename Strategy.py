# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 17:05:09 2019
@author: kosuke

mmbotxxxのストラテジーを移植する

"""

from BitflyerExchange import BitflyerExchange
#import pandas as pd

class Strategy:
    def __init__(self, sqlite3_file_path):
        self.__exchange = BitflyerExchange(sqlite3_file_path)
        self.params = {}
#        self.position = {}

    # unixtime tでの指値side, price, sizeを決定する
    # 約定した場合はポジションを返す
    def order(self, t):
        # 過去deltatime[sec]間のbuy/sellのサイズを集計する
        t_dt_ago = t - self.params['deltatime']
        execusions = self.__exchange.get_execusions(t_dt_ago, t)
        if execusions.empty:
            print('execusions is empty')
            return
        else:
            print('execusions size: %s' % execusions.size)
        buy_size = execusions[execusions['side']==0]['size'].sum()
        sell_size = execusions[execusions['side']==1]['size'].sum()
        # buyとsellの2乗差の絶対値を計算する
        delta_d = abs(buy_size**0.5 - sell_size**0.5)
        # フィルタを通らない場合は注文しない
        if delta_d <= self.params['orderfilter']:
            print('delta_d <= orderfileter')
            return
        # return value
        position = {
            'side': 0,
            'price': 0,
            'size': 0
        }
        # latest ticker
        ticker = self.__exchange.get_latest_ticker(t)
        # update bids, asks
        self.__exchange.reconstruct_bids(0, t)
        self.__exchange.reconstruct_asks(0, t)
        
        if buy_size > sell_size:
            # 時刻tでのbest askを得る
            best_ask = ticker['best_ask'][0]
            # 指値を決定する
            order_price = best_ask - self.params['profitspread']
            # 買いの指値注文をする
            is_execusion = self.__exchange.limit_order(0, order_price, self.params['ordersize'])
            if is_execusion:
                position['side'] = 0
                position['price'] = order_price
                position['size'] = self.params['ordersize']
        else:
            # 時刻tでのbest bidを得る
            best_bid = ticker['best_bid'][0]
            # 指値を決定する
            order_price = best_bid + self.params['profitspread']
            # 売りの指値注文をする
            is_execusion = self.__exchange.limit_order(1, order_price, self.params['ordersize'])
            if is_execusion:
                position['side'] = 1
                position['price'] = order_price
                position['size'] = self.params['ordersize']

        return position    
    
    

if __name__ == '__main__':
    dbfile_path = 'C:/workspace/test.sqlite3'
    strategy = Strategy(dbfile_path)
    strategy.params = {
        'ordersize': 0.01,
        'deltatime': 60,
        'orderfilter': 1.0,
        'profitspread': 100,
        'orderbreak': 1,
        'loopinterval': 0.1,           
    }

    exchange = BitflyerExchange(dbfile_path)
    tmin, tmax = exchange.get_time_range_of_ticker()
    
    t = (tmin + tmax) / 2
    position = strategy.order(t)
    
    import numpy as np
    dt = 60
    for i, t in enumerate(np.arange(tmin, tmax, dt)):
        print('%s: %s' % (i, t))
        position = strategy.order(t)
        print(str(position))
        



