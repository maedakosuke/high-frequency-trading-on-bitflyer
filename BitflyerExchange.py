# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 11:22:40 2019
@author: kosuke
シミュレーションをシンプルにするための条件
自分の約定は履歴に反映しない
約定にかかるタイムロスは0
"""

import pandas as pd
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
            self.__dbsystem.read_latest_bids_filtered_by_get_date(t1, t2)
        )
        print('bids size: %s (%s - %s)' % (self.bids.size, t1, t2))


    # スナップショットと差分情報から時刻tの時点のbidsデータを構成する
    # unixtime t1, t2
    def reconstruct_asks(self, t1, t2):
        self.asks = pd.DataFrame(
            self.__dbsystem.read_latest_asks_filtered_by_get_date(t1, t2)
        )
        print('asks size: %s (%s - %s)' % (self.asks.size, t1, t2))


    # 指値注文を受け付ける
    # datetime date, int side, float price, size
    # 約定した場合は約定結果のdictを返す
    # 簡単のためにdictではなくboolを返す
    def limit_order(self, side, price, size):
        if self.bids.empty or self.asks.empty:
            print('bids / asks is empty')
            return

        if side == 0:
            # buy limit orderなので上限以下のasksを参照する
            # 指値より安く買える場合は約定する
            df =  self.asks[(self.asks['price']<=price) & (self.asks['size']>0)]
        elif side == 1:
            # sell limit orderなので下限以上のbidsを参照する
            # 指値より高く売れる場合は約定する
            df = self.bids[(self.bids['price']>=price) & (self.bids['size']>0)]
        else:
            return
        # debug
        print('limit_order(): total size in bids/asks %s' % df['size'].sum())
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


if __name__ == '__main__':
    dbfile_path = 'C:/workspace/test.sqlite3'
    exchange = BitflyerExchange(dbfile_path)

    # executions読み込みテスト
    t1 = tu.time_as_unixtime('2019-02-01 02:30:00.000000') # UTC timezone
    t2 = tu.time_as_unixtime('2019-02-10 02:31:00.000000')
    executions = exchange.get_executions(t1, t2)

    # bids, asks構成テスト
    t1 = tu.time_as_unixtime('2019-02-01 10:30:00.000000') # UTC timezzone
    t2 = tu.time_as_unixtime('2019-02-10 12:31:00.000000')
    exchange.reconstruct_bids(t1, t2)
    exchange.reconstruct_asks(t1, t2)
    bids = exchange.bids
    asks = exchange.asks

    # 指値注文テスト    
    is_execution_success = exchange.limit_order(0, 376000, 0.01)  # BUY
    is_execution_success = exchange.limit_order(1, 376000, 0.01)  # SELL

    # ticker読み込みテスト
    t = tu.time_as_unixtime('2019-02-10 10:30:00.000000') # UTC timezzone
    ticker = exchange.get_latest_ticker(t)
    best_bid = ticker['best_bid'][0]
    best_ask = ticker['best_ask'][0]
    
    # tickerタイムスタンプ読み込みテスト
    tmin, tmax = exchange.get_time_range_of_ticker()
    