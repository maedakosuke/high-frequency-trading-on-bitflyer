# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 11:22:40 2019

@author: kosuke
"""

import pandas as pd
import util.Sqlite3DatabaseSystemForBitflyer as myutil
from util.Sqlite3DatabaseSystemForBitflyer import Sqlite3DatabaseSystemForBitflyer

"""
シミュレーションをシンプルにするための条件
自分の約定は履歴に反映しない
約定にかかるタイムロスは0


"""

class BitflyerExchange:
    # member
    # __dbsystem
    
    def __init__(self, sqlite3_file_path):
        self.__dbsystem = Sqlite3DatabaseSystemForBitflyer(sqlite3_file_path)
    
    
    # 時刻t1-t2の間の約定履歴を提供する
    # datetime t1, t2
    def get_execusions(self, t1, t2):
        execusions = self.__dbsystem.read_execusions_filtered_by_exec_date(t1, t2)
        return pd.DataFrame(execusions)
        
    
    # スナップショットと差分情報から時刻tの時点のbidsデータを構成する
    # datetime t
    def reconstruct_bids(self, t1, t2):
        self.bids = pd.DataFrame(
            self.__dbsystem.read_latest_bids_filtered_by_get_date(t1, t2)
        )


    # スナップショットと差分情報から時刻tの時点のbidsデータを構成する
    # datetime t
    def reconstruct_asks(self, t1, t2):
        self.asks = pd.DataFrame(
            self.__dbsystem.read_latest_asks_filtered_by_get_date(t1, t2)
        )


    # 指値注文を受け付ける
    # datetime date, int side, float price, size
    # 約定した場合は約定結果のdictを返す
    # 簡単のためにdictではなくboolを返す
    def limit_order(self, side, price, size):
        if side == 0:
            # buy limit orderなので上限以下のasksを参照する
            # 指値より安く買える場合は約定する
            df =  self.asks[(self.asks['price']<=price) & (self.asks['size']>0)]
        else:
            # sell limit orderなので下限以上のbidsを参照する
            # 指値より高く売れる場合は約定する
            df = self.bids[(self.bids['price']>=price) & (self.bids['size']>0)]
        # debug
        print('limit_order(): total size in bids/asks %s' % df['size'].sum())
        # 約定に足りるだけのsizeがある場合は約定成功
        return df['size'].sum() >= size
        

    # 時刻tより過去の最も新しいティッカーを返す
    def get_latest_ticker(self, t):
        ticker = self.__dbsystem.read_latest_ticker(t)
        return pd.DataFrame(ticker)




if __name__ == '__main__':
    dbfile_path = 'C:/workspace/test.sqlite3'
    exchange = BitflyerExchange(dbfile_path)

    # execusions読み込みテスト
    t1 = myutil.time_as_datetime('2019-02-01 02:30:00.000000') # UTC timezone
    t2 = myutil.time_as_datetime('2019-02-10 02:31:00.000000')
    execusions = exchange.get_execusions(t1, t2)

    # bids, asks構成テスト
    t1 = myutil.time_as_datetime('2019-02-01 10:30:00.000000') # UTC timezzone
    t2 = myutil.time_as_datetime('2019-02-10 12:31:00.000000')
    exchange.reconstruct_bids(t1, t2)
    exchange.reconstruct_asks(t1, t2)
    bids = exchange.bids
    asks = exchange.asks

    # 指値注文テスト    
    is_execusion_success = exchange.limit_order('BUY', 376000, 0.01)

    # ticker読み込みテスト
    t = myutil.time_as_datetime('2019-02-10 10:30:00.000000') # UTC timezzone
    ticker = exchange.get_latest_ticker(t)
    