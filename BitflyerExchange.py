# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 11:22:40 2019
@author: kosuke
シミュレーションをシンプルにするための条件
自分の約定は履歴に反映しない
約定にかかるタイムロスは0
"""

import numpy as np
import pandas as pd
import util.cnst as cnst
import util.timeutil as tu
from util.Sqlite3DatabaseSystemForBitflyer import Sqlite3DatabaseSystemForBitflyer


class BitflyerExchange:
    def __init__(self, sqlite3_file_path):
        self.__dbsystem = Sqlite3DatabaseSystemForBitflyer(sqlite3_file_path)
    
    
    # 時刻t1-t2の間の約定履歴を提供する
    # unixtime t1, t2
    def get_executions(self, t1, t2):
        executions = self.__dbsystem.read_executions_filtered_by_exec_date(t1, t2)
        return pd.DataFrame(executions)
        
    
    # スナップショットと差分情報から時刻tの時点のbidsデータを構成する
    # unixtime t1, t2
    def reconstruct_bids(self, t1, t2):
        self.bids = pd.DataFrame(
            self.__dbsystem.read_latest_bids_filtered_by_timestamp(t1, t2)
        )
#        print('bids size: %s (%s - %s)' % (self.bids.size, t1, t2))


    # スナップショットと差分情報から時刻tの時点のbidsデータを構成する
    # unixtime t1, t2
    def reconstruct_asks(self, t1, t2):
        self.asks = pd.DataFrame(
            self.__dbsystem.read_latest_asks_filtered_by_timestamp(t1, t2)
        )
#        print('asks size: %s (%s - %s)' % (self.asks.size, t1, t2))


    # 指値注文を受け付ける
    # datetime date, int side, float price, size
    # 約定した場合は約定結果のdictを返す
    # 簡単のためにdictではなくboolを返す
    def limit_order(self, side, price, size):
        if self.bids.empty or self.asks.empty:
            print('limit_order() bids/asks empty')
            return

        if side == cnst.BUY:
            # buy limit orderなので上限以下のasksを参照する
            # 指値より安く買える場合は約定する
            df =  self.asks[(self.asks['price']<=price) & (self.asks['size']>0)]
            # debug
            print('limit_order() filtered-asks sum(size)', df['size'].sum())
        elif side == cnst.SELL:
            # sell limit orderなので下限以上のbidsを参照する
            # 指値より高く売れる場合は約定する
            df = self.bids[(self.bids['price']>=price) & (self.bids['size']>0)]
            # debug
            print('limit_order() filtered-bids sum(size)', df['size'].sum())
        else:
            return
        # 約定に足りるだけのsizeがある場合は約定成功
        return df['size'].sum() >= size
        

    # unixtime tより過去の最も新しいティッカーを返す
    def get_latest_ticker(self, t):
        ticker = self.__dbsystem.read_latest_ticker(t)
        return pd.DataFrame(ticker)


    # tickerテーブルのtimestampの範囲を返す
    def get_time_range_of_ticker(self):
        tminmax = pd.DataFrame(
            self.__dbsystem.read_min_max_timestamp_of_ticker()
        )
        tmin = tminmax['min(timestamp)'][0]
        tmax = tminmax['max(timestamp)'][0]
        return tmin, tmax


    # self.bids内のbest bidを返す
    def best_bid_in_constructed_bids(self):
        if self.bids.empty:
            print('best_bid_in_constructed_bids() bids empty')
            return
        return self.bids[self.bids['size']>0]['price'].max()


    # self.asks内のbest askを返す
    def best_ask_in_constructed_asks(self):
        if self.asks.empty:
            print('best_ask_in_constructed_asks() asks empty')
            return
        return self.asks[self.asks['size']>0]['price'].min()


    # unixtime t1-t2間のtickerを返す
    def get_ticker(self, t1, t2):
        ticker = self.__dbsystem.read_ticker_filtered_by_timestamp(t1, t2)
        return pd.DataFrame(ticker)


if __name__ == '__main__':
    dbfile_path = 'C:/workspace/test.sqlite3'
    exchange = BitflyerExchange(dbfile_path)

    # executions読み込みテスト
    t1 = tu.time_as_unixtime('2019-02-01 02:30:00.000000') # UTC timezone
    t2 = tu.time_as_unixtime('2019-02-10 02:31:00.000000')
    executions = exchange.get_executions(t1, t2)

    # bids, asks構成テスト
    t1 = tu.time_as_unixtime('2019-02-01 10:30:00.000000') 
    t2 = tu.time_as_unixtime('2019-02-10 15:10:00.000000') # UTC timezzone
    exchange.reconstruct_bids(0, t2 - 9*60*60)
    exchange.reconstruct_asks(0, t2 - 9*60*60)
    bids = exchange.bids
    asks = exchange.asks
    best_bid = exchange.best_bid_in_constructed_bids()
    best_ask = exchange.best_ask_in_constructed_asks()

    # 指値注文テスト    
    is_execution_success = exchange.limit_order(cnst.BUY, 376000, 0.01)  # BUY
    is_execution_success = exchange.limit_order(cnst.SELL, 376000, 0.01)  # SELL

    # ticker読み込みテスト
#    t = tu.time_as_unixtime('2019-02-10 10:30:00.000000') # UTC timezzone
#    ticker = exchange.get_latest_ticker(t)
#    best_bid = ticker['best_bid'][0]
#    best_ask = ticker['best_ask'][0]
    
    # tickerタイムスタンプ読み込みテスト
    tmin, tmax = exchange.get_time_range_of_ticker()


    # best bid, best ask time dependency
#    t1 = tu.time_as_unixtime('2019-02-10 13:30:00.000000') - 9*60*60
#    t2 = tu.time_as_unixtime('2019-02-10 16:00:00.000000') - 9*60*60
#    best_bid = []
#    best_ask = []
#    timestamps = []
#    for i, t in enumerate(np.arange(t1, t2, 600)):
#        print(i, tu.time_as_text(t))
#        exchange.reconstruct_bids(0, t)
#        exchange.reconstruct_asks(0, t)
#        timestamps.append(t)
#        best_bid.append(exchange.best_bid_in_constructed_bids())
#        best_ask.append(exchange.best_ask_in_constructed_asks())
#        
#    df = pd.DataFrame(timestamps, best_bid, best_ask, columns=['t', 'bid', 'ask'])
    
    # ticker読み込みテスト2
    ticker = exchange.get_ticker(tmin, tmax)
    
    