# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 21:18:05 2019

@author: kosuke
"""

import sys
import socketio
import util.timeutil as tu
import util.cnst as cnst
from dateutil.parser import parse
import pandas as pd
import numpy as np
import math
from scipy import ndimage
import util.plotutil as pu
import util.bitflyer_order as bf

pd.options.display.float_format = '{:.3f}'.format

if len(sys.argv) != 3:
    pass
#    print('Usage: python bitflyer_socketio.py <sqlite3 file path> <channel name>')
#    sys.exit(1)


class BitflyerBoard:
    def __init__(self):
        # Realtimeで取得する板データの格納
        self.lst_orderbook_cols = ['price','size','t']
        self.df_bids = pd.DataFrame(columns=self.lst_orderbook_cols)
        self.df_asks = pd.DataFrame(columns=self.lst_orderbook_cols)
        # 板データから求めるデプスの一部
        # p1k: ltpからプラス1000JPYのボリュームの累積和(n1kはマイナス)
        self.lst_depth_cols = ['t','ltp','n1k','p1k','n5k','p5k','n10k','p10k']
        self.df_depth = pd.DataFrame(columns=self.lst_depth_cols)


    def store(self, dct_orderbook):
        now = tu.now_as_unixtime();

        # mid price処理
        # なぞのBest bidとBest askの交差を防止する
        mp = dct_orderbook['mid_price']
        self.df_bids.loc[self.df_bids['price'] >= mp, 'size'] = 0
        self.df_asks.loc[self.df_asks['price'] <= mp, 'size'] = 0

        for bid in dct_orderbook['bids']:
            p = bid['price']
            s = bid['size']
            df = self.df_bids[self.df_bids['price']==p]
            len_df = len(df)
            if len_df == 0:
                # append new price and size
                self.df_bids = self.df_bids.append(
                    {'price': p, 'size': s, 't': now},
                    ignore_index=True)
            elif len_df == 1:
                # update size
                idx = df.index[0]
                self.df_bids['size'][idx] = s
                self.df_bids['t'][idx] = now
            else:
                # アサートされる場合は2つ以上スレッドが走っているので再起動する
                assert(0)

        for ask in dct_orderbook['asks']:
            p = ask['price']
            s = ask['size']
            df = self.df_asks[self.df_asks['price']==p]
            len_df = len(df)
            if len_df == 0:
                # append new price and size
                self.df_asks = self.df_asks.append(
                    {'price': p, 'size': s, 't': now},
                    ignore_index=True)
            elif len_df == 1:
                # update size
                idx = df.index[0]
                self.df_asks['size'][idx] = s
                self.df_asks['t'][idx] = now
            else:
                assert(0)


    def make_depth(self, time):
        # デプスチャート全体の時系列データは大きいので
        # ポイントの時系列データのみ蓄積する

        bids = self.df_bids[self.df_bids['size'] > 0]
        asks = self.df_asks[self.df_asks['size'] > 0]

        ltp = (bids['price'].max() + asks['price'].min()) / 2

        depth_n1k = bids[bids['price'] >= ltp - 1000]['size'].sum()
        depth_p1k = asks[asks['price'] <= ltp + 1000]['size'].sum()
        depth_n5k = bids[bids['price'] >= ltp - 5000]['size'].sum()
        depth_p5k = asks[asks['price'] <= ltp + 5000]['size'].sum()
        depth_n10k = bids[bids['price'] >= ltp - 10000]['size'].sum()
        depth_p10k = asks[asks['price'] <= ltp + 10000]['size'].sum()

        self.df_depth = self.df_depth.append(
            {'t': time,
             'ltp': ltp,
             'n1k': depth_n1k, 'p1k': depth_p1k,
             'n5k': depth_n5k, 'p5k': depth_p5k,
             'n10k': depth_n10k, 'p10k': depth_p10k},
             ignore_index=True)

    def bids(self):
        # サイズ0の情報が残っているから
        df = self.df_bids[self.df_bids['size'] > 0]
        df = df.sort_values('price', ascending=False)
        return df.copy()

    def asks(self):
        df = self.df_asks[self.df_asks['size'] > 0]
        df = df.sort_values('price', ascending=True)
        return df.copy()

    def depth(self):
        return self.df_depth.copy()


board = BitflyerBoard()

sio = socketio.Client(
    reconnection_delay=10,
    reconnection_delay_max=10,
    randomization_factor=0.0,
    logger=False)


@sio.on('connect')
def on_connect():
    print(tu.now_as_text(), 'connected')

@sio.on('lightning_board_FX_BTC_JPY')
def on_board(data):
    try:
        if data:
            board.store(data)
    except TimeoutError as e:
        print(tu.now_as_text(), 'TimeoutError')
        print(e, file=sys.stderr)

@sio.on('lightning_board_snapshot_FX_BTC_JPY')
def on_board_snapshot(data):
    try:
        if data:
            print(tu.now_as_text(), 'on_board_snapshot')
            board.store(data)
            sio.emit('unsubscribe', 'lightning_board_snapshot_FX_BTC_JPY')
    except TimeoutError as e:
        print(tu.now_as_text(), 'TimeoutError')
        print(e, file=sys.stderr)

@sio.on('disconnect')
def on_disconnect():
    print(tu.now_as_text(), 'disconnected')


sio.connect('https://io.lightstream.bitflyer.com', transports=['websocket'])
sio.emit('subscribe', 'lightning_board_snapshot_FX_BTC_JPY')
tu.sleep(2)
sio.emit('subscribe', 'lightning_board_FX_BTC_JPY')
sleep_time = 10
ii_dt = 12
th = 100
i = 0
own_side = 0  # 所有ポジションのサイド
order_size = 0.1
end_time =  tu.text_to_unixtime('2019-04-14 19:00:00.000000')
while(True):
    if len(board.df_bids) == 0 or len(board.df_asks) == 0:
        tu.sleep(sleep_time)
        continue

    board.make_depth(tu.now_as_unixtime())

    # watch depth chart
    bids = board.bids()
    asks = board.asks()
    pu.plot_depth_live(bids['price'], bids['size'].cumsum(), asks['price'], asks['size'].cumsum(), 1000)

    # bot function start -----------------
    if i <= ii_dt:
        tu.sleep(sleep_time)
        i += 1
        continue

    depth = board.depth()
    ii_now = (depth.p1k - depth.n1k).diff(ii_dt).tail(1).values[0]

    if ii_now < 0:
        if own_side == cnst.BUY:
            print(tu.now_as_text(), 'keep buy position', ii_now)
        else:
            if ii_now < -th:
                print(tu.now_as_text(), 'buy new position', ii_now)
                if own_side == cnst.SELL:
                    bf.order('BUY', order_size * 2)
                else:
                    bf.order('BUY', order_size)
                own_side = cnst.BUY
            else:
                if own_side == cnst.SELL:
                    print(tu.now_as_text(), 'close sell position', ii_now)
                    bf.order('BUY', order_size)
                    own_side = 0
                else:
                    print(tu.now_as_text(), 'wait...', ii_now)
    elif ii_now > 0:
        if own_side == cnst.SELL:
            print(tu.now_as_text(), 'keep sell position', ii_now)
        else:
            if ii_now > th:
                print(tu.now_as_text(), 'sell new position', ii_now)
                ret = {}
                if own_side == cnst.BUY:
                    bf.order('SELL', order_size * 2)
                else:
                    bf.order('SELL', order_size)
                own_side = cnst.SELL
            else:
                if own_side == cnst.BUY:
                    print(tu.now_as_text(), 'close buy position', ii_now)
                    bf.order('SELL', order_size)
                    own_side = 0
                else:
                    print(tu.now_as_text(), 'wait...', ii_now)
    # bot function end -----------------

    tu.sleep(sleep_time)
    i += 1

    if tu.now_as_unixtime() > end_time:
        sio.emit('unsubscribe', 'lightning_board_snapshot_FX_BTC_JPY')
        sio.emit('unsubscribe', 'lightning_board_FX_BTC_JPY')
        sio.disconnect()
        break

depth = board.depth()

pu.plot_2curves(depth.p10k - depth.n10k, depth.ltp, 'depth.p10k - depth.n10k', 'ltp')
pu.plot_2curves(depth.p5k - depth.n5k, depth.ltp, 'depth.p5k - depth.n5k', 'ltp')
pu.plot_2curves(depth.p1k - depth.n1k, depth.ltp, 'depth.p1k - depth.n1k', 'ltp')

# 最適なdx(デプスの時間変化の過去dt), dy
w, h = 30, 30;
ic_mat = [[0 for x in range(w)] for y in range(h)]
for y in range(h):
    for x in range(w):
        ic = np.corrcoef((depth.p1k - depth.n1k).diff(x)[x+1:-(y+1)], -depth.ltp.diff(-y)[x+1:-(y+1)])[0, 1]
        ic_mat[y][x] = ic
        print(x, y, ic)
from matplotlib import pyplot as plt
plt.imshow(ic_mat)

ii_dt = 10
dp_dt = 10

ii = (depth.p1k - depth.n1k).diff(ii_dt)
dp = -depth.ltp.diff(-dp_dt)
pu.plot_hist(ii.dropna())
pu.plot_corr(ii[ii_dt+1:-(dp_dt+1)], dp[ii_dt+1:-(dp_dt+1)])
pu.plot_2curves(depth.ltp, -dp)
pu.plot_2curves(depth.ltp, depth.ltp.diff(dp_dt))
pu.plot_2curves(depth.p1k - depth.n1k, ii)


th = 100

buy_index = depth[ii < -th].index
pnl = dp[buy_index]

# total asset
pnl.sum()

# trade count
len(pnl)

# balance curve
pu.plot_1curve(np.cumsum(pnl))

# 期待値
pnl.sum() / len(pnl)

# PF
-1 * pnl[pnl > 0].sum() / pnl[pnl < 0].sum()

# 勝率
(pnl > 0).sum() / len(pnl)

# 60分当たり取引回数
