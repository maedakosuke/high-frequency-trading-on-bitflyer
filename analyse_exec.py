# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 19:09:16 2019

@author: kosuke
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from BitflyerExchange import BitflyerExchange
import util.cnst as cnst
import util.timeutil as tu
import util.plotutil as pu

import seaborn as sns
import statsmodels.api as sm

dbfile_path = 'C:/workspace/bf20190326.sqlite3'
exchange = BitflyerExchange(dbfile_path)
tmin, tmax = exchange.get_time_range_of_ticker()

executions = exchange.get_executions(tmin, tmax)
buys = executions[executions.side==cnst.BUY]
sells = executions[executions.side==cnst.SELL]
plt.plot(buys['size'])
plt.plot(sells['size'])
plt.plot(buys['price'])
plt.plot(sells['price'])

# 0.01の約定が飛び抜けて多い
plt.hist(buys[buys['size']<0.5]['size'], 100)


bs = buys[::50]
ss = sells[::50]

plt.plot(bs['size'])
plt.plot(bs['price'])
plt.plot(ss['size'])

buys_acf = sm.tsa.stattools.acf(bs['size'], nlags=40)
plt.plot(buys_acf)
sells_scf = sm.tsa.stattools.acf(ss['size'], nlags=40)
plt.plot(sells_scf)


#  自己相関のグラフ
fig = plt.figure(figsize=(12,8))
ax1 = fig.add_subplot(211)
fig = sm.graphics.tsa.plot_acf(bs['size'], lags=40, ax=ax1)
ax2 = fig.add_subplot(212)
fig = sm.graphics.tsa.plot_acf(ss['size'], lags=40, ax=ax2)



buys_ccf = sm.tsa.stattools.ccf(bs['size'], bs['price'])
plt.plot(buys_ccf)
sells_ccf = sm.tsa.stattools.ccf(ss['size'], ss['price'])
plt.plot(sells_ccf)


cc = sm.tsa.stattools.ccf(bs['price'], bs['size'])
plt.plot(cc)


# make 1 second candle data

buy_execs = executions[(executions['id']>0) & (executions['side']==cnst.BUY) & (executions['size']<1.0)]
buy_execs = buy_execs.sort_values('exec_date')
sell_execs = executions[(executions['id']>0) & (executions['side']==cnst.SELL) & (executions['size']<1.0)]
sell_execs = sell_execs.sort_values('exec_date')


t1 = int(buy_execs['exec_date'].min())
t2 = int(buy_execs['exec_date'].max())
print(t2-t1)


def make_ohlcv(execs, t1, t2):
    df = execs[(execs['exec_date']>=t1) & (execs['exec_date']<t2)]
    if len(df) == 0:
        return None
    o = df.head(1)['price'].values[0]
    h = df['price'].max()
    l = df['price'].min()
    c = df.tail(1)['price'].values[0]
    v = df['size'].sum()
    return o, h, l, c, v

buy_tohlcv_1 = pd.DataFrame(columns=['t','o','h','l','c','v'])
sell_tohlcv_1 = pd.DataFrame(columns=['t','o','h','l','c','v'])
dt = 1.0
for t in np.arange(t1, t2, dt):
    buy_ohlcv = make_ohlcv(buy_execs, t, t+dt)
    sell_ohlcv = make_ohlcv(sell_execs, t, t+dt)
    if buy_ohlcv == None or sell_ohlcv == None:
        continue
    buy_tohlcv_1 = buy_tohlcv_1.append(
        {'t': t, 'o': buy_ohlcv[0], 'h': buy_ohlcv[1], 'l': buy_ohlcv[2], 'c': buy_ohlcv[3], 'v': buy_ohlcv[4]},
        ignore_index=True)
    sell_tohlcv_1 = sell_tohlcv_1.append(
        {'t': t, 'o': sell_ohlcv[0], 'h': sell_ohlcv[1], 'l': sell_ohlcv[2], 'c': sell_ohlcv[3], 'v': sell_ohlcv[4]},
        ignore_index=True)

buys_ccf = sm.tsa.stattools.ccf(buy_tohlcv_1.v, buy_tohlcv_1.o)
plt.plot(buys_ccf)
sells_ccf = sm.tsa.stattools.ccf(sell_tohlcv_1.v, sell_tohlcv_1.o)
plt.plot(sells_ccf)
bs_ccf = sm.tsa.stattools.ccf((buy_tohlcv_1.v - sell_tohlcv_1.v)[11:], buy_tohlcv_1.o.diff(10)[11:])
plt.plot(((buy_tohlcv_1.v - sell_tohlcv_1.v)[111:311]/10).values)
plt.plot((buy_tohlcv_1.o.diff(10)[111:311]/200).values)
plt.plot(bs_ccf[100:300]*30)
plt.show()

plt.scatter(bs_ccf, buy_tohlcv_1.o.diff(10)[11:], 1)

pu.plot_corr((buy_tohlcv_1.v**0.5-sell_tohlcv_1.v**0.5)[:-11],buy_tohlcv_1.o.diff(-10)[:-11],False)
pu.plot_corr((buy_tohlcv_1.v**0.5-sell_tohlcv_1.v**0.5)[:-21],buy_tohlcv_1.o.diff(-20)[:-21],False)

plt.figure(figsize=(10,5),dpi=100)
plt.plot((buy_tohlcv_1.v**0.5-sell_tohlcv_1.v**0.5)[11:311])
plt.plot(buy_tohlcv_1.o.diff(10)[11:311]/100)
plt.show()

plt.figure(figsize=(10,5),dpi=100)
plt.plot((buy_tohlcv_1.v**0.5-sell_tohlcv_1.v**0.5).diff(-10)[11:311])
plt.plot(buy_tohlcv_1.o.diff(10)[11:311]/100)
plt.show()
pu.plot_corr((buy_tohlcv_1.v**0.5-sell_tohlcv_1.v**0.5).diff(-10)[11:-11],buy_tohlcv_1.o.diff(10)[11:-11])

plt.figure(figsize=(10,5),dpi=100)
plt.plot((buy_tohlcv_1.v**0.5-sell_tohlcv_1.v**0.5).diff(-10)[311:611])
plt.plot(buy_tohlcv_1.o.diff(10)[311:611]/100)
plt.show()

plt.figure(figsize=(10,5),dpi=100)
plt.plot((buy_tohlcv_1.v**0.5-sell_tohlcv_1.v**0.5).diff(-10)[11:311])
plt.plot((buy_tohlcv_1.o[11:311] - buy_tohlcv_1.o.mean())/100)
plt.show()

plt.figure(figsize=(10,5),dpi=100)
plt.plot((buy_tohlcv_1.v**0.5-sell_tohlcv_1.v**0.5).diff(-10)[311:611])
plt.plot((buy_tohlcv_1.o[311:611] - buy_tohlcv_1.o.mean())/100)
plt.show()

# 1 sec tohlcv to n sec tohlcv
def remake_tohlcv(tohlcv, n):
    remaked = pd.DataFrame(columns=['t','o','h','l','c','v'])
    for i in range(1, len(tohlcv), n):
        remaked = remaked.append(
            {'t': tohlcv.head(i).t, 'o': tohlcv.head(i).o, 'h': tohlcv[i:n].h.max(), 'l': tohlcv[i:n].l.min(), 'c': tohlcv.head(i+n-1).c, 'v': tohlcv[i:n].v.sum()},
            ignore_index=True)
    return remaked

buy_tohlcv_5 = remake_tohlcv(buy_tohlcv_1, 5)
sell_tohlcv_5 = remake_tohlcv(sell_tohlcv_1, 5)


pu.plot_corr((buy_tohlcv_1.v**0.5-sell_tohlcv_1.v**0.5)[:-11],buy_tohlcv_1.o.diff(-10)[:-11])
pu.plot_corr((buy_tohlcv_5.v**0.5-sell_tohlcv_5.v**0.5)[:-11],buy_tohlcv_5.o.diff(-10)[:-11])


ret=sm.tsa.stattools.ccf(buy_tohlcv_1.v.diff(-10)[:-11],buy_tohlcv_1.o.diff(-10)[:-11])
plt.figure(figsize=(10,8),dpi=100)
plt.plot(buy_tohlcv_1.v.diff(-10)[1:300]/10)
plt.plot(buy_tohlcv_1.o.diff(-10)[1:300]/200)
plt.plot(ret[1:300]*30)
plt.show()

pu.plot_corr(buy_tohlcv_1.v.diff(-1)[:-11].values,buy_tohlcv_1.o.diff(10)[11:].values,False)


# smooth
from scipy import ndimage
y1 = buy_tohlcv_1.v**0.5 - sell_tohlcv_1.v**0.5
y1_s = ndimage.filters.gaussian_filter(y1,3)
y1_sma = y1.rolling(5).mean()
y1_ema = y1.ewm(span=5, adjust=False).mean()
y1_s_d = np.diff(y1_s, 10)
y2 = buy_tohlcv_1.o.shift(-4).diff(-10)  # 4秒遅延, 10秒の価格変化
y2 = buy_tohlcv_1.o.diff(-10)

pu.plot_2curves(y1_s, y2, 'buy**0.5 - sell**0.5 smooth 3', 'open diff 10sec after')
pu.plot_2curves(y1, y1_sma, 'buy**0.5 - sell**0.5', 'sma 10')
pu.plot_2curves(y1_ema, y2, 'buy**0.5 - sell**0.5 ema 5', 'price diff 10')
y1_ema_s = ndimage.filters.gaussian_filter(y1_ema,3)
pu.plot_2curves(y1_ema_s, y2, 'buy**0.5 - sell**0.5 ema 5 s', 'price diff -10')

# zero poin entry/exit strategy test
y1_ema_p = (y1_ema < 0) * 1.0
y1_ema_p = np.diff(y1_ema_p)
pnl = y1_ema_p * y2.shift(-4)[:-1]
pnl.sum()
pu.plot_1curve(np.cumsum(pnl))


y1_ema_s_p = (y1_ema_s < 0) * 1.0
y1_ema_s_p = np.diff(y1_ema_s_p)
#y1_ema_s_p[0] = 5
#y1_ema_s_p[1] = -5
pu.plot_2curves(y1_ema_s_p, y2, 'buy**0.5 - sell**0.5 ema 5 s > 0', 'price diff -10')
pnl = y1_ema_s_p * y2.shift(0)[:-1]
pu.plot_1curve(pnl)
# balance curve
pu.plot_1curve(np.cumsum(pnl))
# 期待値
pnl.sum() / (pnl != 0).sum()
# PF
-1 * pnl[pnl > 0].sum() / pnl[pnl < 0].sum()
# 勝率
(pnl > 0).sum() / (pnl != 0).sum()
# 1分当たり取引回数
60 * (pnl != 0).sum() / len(pnl)

y1_ema_d = y1_ema.diff(-1)
y1_ema_d_s = ndimage.filters.gaussian_filter(y1_ema_d,3)
pu.plot_2curves(y1_ema_d_s, y2, 'buy**0.5 - sell**0.5 ema 5 diff -1 s', 'price diff -10')

pu.plot_hist(y1[:-11])
pu.plot_corr(y1[:-30], y2[:-30])

pu.plot_hist(y1_s[:-11])
pu.plot_corr(y1_s[:-11],y2[:-11])

pu.plot_hist(y1_sma[11:])
pu.plot_corr(y1_sma[11:-11], y2[11:-11])

pu.plot_hist(y1_ema[11:])
pu.plot_corr(y1_ema[11:-11], y2[11:-11])

pu.plot_2curves(y1, y1_s)

ret=sm.tsa.stattools.ccf(y1_s[:-11], y2[:-11])
pu.plot_1curve(ret)
