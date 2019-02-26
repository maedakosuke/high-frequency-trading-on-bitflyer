# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 17:05:09 2019
@author: kosuke

mmbotxxxのストラテジーを移植する

"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import util.cnst as cnst
import util.timeutil as tu
from BitflyerExchange import BitflyerExchange


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
        executions = self.__exchange.get_executions(t_dt_ago, t)
        if executions.empty:
            print('order() executions empty')
            return
        else:
            print('order() executions.size', executions.size)
        buy_size = executions[executions['side']==cnst.BUY]['size'].sum()
        sell_size = executions[executions['side']==cnst.SELL]['size'].sum()
        # buyとsellの2乗差の絶対値を計算する
        delta_d = abs(buy_size**0.5 - sell_size**0.5)
        print('order() buy_size', buy_size, 'sell_size', sell_size, 'delta_d', delta_d)
        # フィルタを通らない場合は注文しない
        if delta_d <= self.params['orderfilter']:
            print('order() delta_d <= orderfileter')
            return
        # return value
        position = {
            'timestamp': t,
            'buy_size': buy_size,
            'sell_size': sell_size,
            'delta_d': delta_d,
            'side': 0,
            'price': 0,
            'size': 0
        }
        # latest ticker
        ticker = self.__exchange.get_latest_ticker(t)
        # update bids, asks
        self.__exchange.reconstruct_bids(t-1000, t)
        self.__exchange.reconstruct_asks(t-1000, t)
        
        if buy_size > sell_size:
            # 時刻tでのbest askを得る
#            best_ask = ticker['best_ask'][0]
            best_ask = self.__exchange.best_ask_in_constructed_asks()
            if best_ask is None or best_ask == 0:
                return
            # 指値を決定する
            order_price = best_ask - self.params['profitspread']
            # 買いの指値注文をする
            is_execution = self.__exchange.limit_order(cnst.BUY, order_price, self.params['ordersize'])
            if is_execution:
                position['side'] = cnst.BUY
                position['price'] = order_price
                position['size'] = self.params['ordersize']
        else:
            # 時刻tでのbest bidを得る
#            best_bid = ticker['best_bid'][0]
            best_bid = self.__exchange.best_bid_in_constructed_bids()
            if best_bid is None or best_bid == 0:
                return
            # 指値を決定する
            order_price = best_bid + self.params['profitspread']
            # 売りの指値注文をする
            is_execution = self.__exchange.limit_order(cnst.SELL, order_price, self.params['ordersize'])
            if is_execution:
                position['side'] = cnst.SELL
                position['price'] = order_price
                position['size'] = self.params['ordersize']

        return position    
    
    
def total_btc(positions, n):
    return (positions['side'] * positions['size']).head(n).sum()


def total_jpy(positions, n):
    return (positions['price'] * (-1 * positions['side']) * positions['size']).head(n).sum()


def final_asset(positions):
    n = len(positions)
    btc = total_btc(positions, n)
    jpy = total_jpy(positions, n)
    ltp = positions[-1:]['price'].values[0]
    asset = jpy + ltp * btc
    print('total btc    :', btc, '[BTC]')
    print('total jpy    :', jpy, '[JPY]')
    print('ltp          :', ltp, '[JPY]')
    print('asset        :', asset, '[JPY]')
    

def asset(positions, n):
    btc = total_btc(positions, n)
    jpy = total_jpy(positions, n)
    ltp = positions.iloc[n-1]['price']
    return jpy + ltp * btc


#def plot_position():


if __name__ == '__main__':
    dbfile_path = 'C:/workspace/test.sqlite3'
    strategy = Strategy(dbfile_path)
    strategy.params = {
        'ordersize': 0.01,    # [BTC]
        'deltatime': 15,      # [s]
        'orderfilter': 0,     # [BTC]^0.5
        'profitspread': -20,  # [JPY]
        'orderbreak': 0,      # [s] 未使用
        'loopinterval': 0,  # [s] 未使用
    }

    exchange = BitflyerExchange(dbfile_path)
    tmin, tmax = exchange.get_time_range_of_ticker()
    
    dt = 1
    positions = []
    for i, t in enumerate(np.arange(tmin, tmax, dt)):
        print('----------*----------*----------*----------')
        print(i, tu.time_as_text(t))
        position = strategy.order(t)
        if position is not None:
            positions.append(position)
        print(position)
        
    positions = pd.DataFrame(positions)
    positions_execution_fail = positions[positions.side==0]
    positions = positions[positions.side!=0]
    assets=[asset(positions, n) for n in range(1,len(positions)+1)]
    positions['asset'] = pd.Series(assets, index=positions.index)
    final_asset(positions)

    
    fig = plt.figure(figsize=(6,4), dpi=300)
    ax = fig.add_subplot(1,1,1)
    ax.set_title('title')
    ax.set_xlabel('timestamp')
    ax.set_ylabel('size')
    #    ax.set_xlim([380000, 381000])
    ax.set_ylim([-2500, 7500])
#    ax.plot((positions['timestamp']-positions['timestamp'].min())/3600, positions['buy_size'],  c='red', label='buy size 15s')
#    ax.plot((positions['timestamp']-positions['timestamp'].min())/3600, positions['sell_size'], c='blue', label='sell size 15s')
    ax.plot((positions.timestamp-positions.timestamp.min())/3600, positions.asset, c='black', label='asset')
    ax.legend()
    fig.savefig('C:/workspace/out.png')

    # 投資指標のヒストグラム
    plt.hist(positions.buy_size - positions.sell_size, bins=int(len(positions)**0.5))
    plt.hist(positions.buy_size**0.5 - positions.sell_size**0.5, bins=int(len(positions)**0.5))
    