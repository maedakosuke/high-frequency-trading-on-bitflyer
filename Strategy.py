# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 17:05:09 2019
@author: kosuke

mmbotxxxのストラテジーを移植する

"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from BitflyerExchange import BitflyerExchange
import util.cnst as cnst
import util.timeutil as tu
import util.plotutil as pu

dbfile_path = 'C:/workspace/test.sqlite3'
exchange = BitflyerExchange(dbfile_path)
tmin, tmax = exchange.get_time_range_of_ticker()

# mmbotxxxのストラテジーの移植
# unixtime tでの指値side, price, sizeを決定する
# ポジションを返す
def simulate_trade(params, t):
    # return value
    position = pd.Series({
        'order_time': t,  # 注文時刻 unixtime [s]
        'exec_size': None,  # 約定数
        'exec_buy_size': None,  # 約定の積算買い注文サイズ [BTC]
        'exec_sell_size': None,  # 約上の積算売り注文サイズ [BTC]
        'invest_index': None,  # 投資指標 [BTC]^0.5
        'order_side': None,  # cnst.BUY:買い注文 cnst.SELL:売り注文
        'order_price': None,  # 注文金額 [JPY]
        'order_size': None,  # 注文サイズ [BTC]
        'close_time': None,  # 注文精算時刻 unixtime [s]
        'close_side': None,  # 注文の反対取引 cnst.BUY:買い, cnst.SELL:売り
        'close_price': None,  # 注文精算金額 [JPY]
        'pnl': None,          # 損益 [JPY]
        })
    # 積算時間のbuy/sellのサイズを集計する
    executions = exchange.get_executions(t - params.integral_time, t)
    if executions.empty:
        #print('order() executions empty')
        return position
    else:
        #print('order() executions.size', executions.size)
        pass
    buy_size = executions[executions.side == cnst.BUY]['size'].sum()
    sell_size = executions[executions.side == cnst.SELL]['size'].sum()
    #buy_size = executions[executions.side == cnst.BUY].size
    #sell_size = executions[executions.side == cnst.SELL].size

    # buyとsellの2乗差の絶対値(Absolute Square Diff.)(投資指標i.i.)を計算する
    asd = abs(buy_size**0.5 - sell_size**0.5)
    #print('order() buy_size', buy_size, 'sell_size', sell_size, 'i.i.', asd)

    position.exec_size = executions.size
    position.exec_buy_size = buy_size
    position.exec_sell_size = sell_size
    position.invest_index = asd

    # フィルタを通らない場合は注文しない
    if asd < params.filter_low or params.filter_high < asd:
        #print('order() investment index does not pass the filter')
        return position

    # t時点の板情報をDBから構成する
    exchange.construct_bids(t - 1000, t)
    exchange.construct_asks(t - 1000, t)
    # 注文判定
    if buy_size > sell_size:
        # 時刻tでのbest askを得る
        best_ask = exchange.best_ask_in_constructed_asks()
        if best_ask is None or best_ask == 0:
            return position
        # 指値を決定する
        order_price = best_ask - params.profit_spread
        # 買いの指値注文をする
        is_execution = exchange.limit_order(
            cnst.BUY, order_price, params.order_size)
        if is_execution:
            position.order_side = cnst.BUY
            position.order_price = order_price
            position.order_size = params.order_size
    else:
        # 時刻tでのbest bidを得る
        best_bid = exchange.best_bid_in_constructed_bids()
        if best_bid is None or best_bid == 0:
            return position
        # 指値を決定する
        order_price = best_bid + params.profit_spread
        # 売りの指値注文をする
        is_execution = exchange.limit_order(
            cnst.SELL, order_price, params.order_size)
        if is_execution:
            position.order_side = cnst.SELL
            position.order_price = order_price
            position.order_size = params.order_size

    # 成行注文で反対取引をする
    # TODO この反対取引も注文と同じように板情報を再構成したほうが良い?
    if is_execution:
        tc = t + params.close_time
        ticker = exchange.get_latest_ticker(tc)
        if abs(ticker.timestamp - tc) > 10:
            # 取引失敗
            #print('order() failed to close the position')
            return position
        position.close_time = ticker.timestamp
        if position.order_side == cnst.BUY:
            position.close_side = cnst.SELL
            position.close_price = ticker.best_bid
        elif position.order_side == cnst.SELL:
            position.close_side = cnst.BUY
            position.close_price = ticker.best_ask
        # 損益計算
        position.pnl = position.order_size * position.order_side * (position.close_price - position.order_price)

    return position


# paramsを入力して最終損益を計算する
# dict params
def total_asset(params):
    tstart = tmin  # 計算の開始時刻 unixtime [s]
    tend = tmax  # 計算の終了時刻 unixtime [s]
    tstep = 60  # 取引の時間間隔 [s]

    positions = pd.DataFrame()
    for i, t in enumerate(np.arange(tstart, tend, tstep)):
        position = simulate_trade(pd.Series(params), t)
        positions = positions.append(position, ignore_index=True)

    # 成功取引のみ抽出
    p = positions[~positions.close_side.isnull()]
    return p.pnl.sum()


if __name__ == '__main__':
    params = pd.Series({
        'order_size': 0.01,  # 注文サイズ [BTC]
        'integral_time': 60,  # 投資指標(約定履歴)の積算時間 [s]
        'filter_low': 0,  # 注文判定に使用する投資指標の閾値Low [BTC]^0.5
        'filter_high': 100,  # 閾値High [BTC]^0.5
        'profit_spread': 0,  # 注文金額のbest bid/askからの差 [JPY]
        'close_time': 60,  # closetime秒後に反対取引をしてポジションを精算する [s]
    })
    tstart = tmin  # 計算の開始時刻 unixtime [s]
    tend = tmax  # 計算の終了時刻 unixtime [s]
    tstep = 120  # 取引の時間間隔 [s]

    positions = pd.DataFrame()
    for i, t in enumerate(np.arange(tstart, tend, tstep)):
        if i % 60 == 0:
            print('----------*----------*----------*----------')
            print(i, tu.time_as_text(t))
        position = simulate_trade(params, t)
        positions = positions.append(position, ignore_index=True)

    # 成功取引のみ抽出
    p = positions[~positions.close_side.isnull()]

    # 投資指標vsリターン散布図
    pu.plot_scatter(p.exec_buy_size**0.5 - p.exec_sell_size**0.5, p.pnl, False)
    pu.plot_scatter(p.exec_buy_size - p.exec_sell_size, p.pnl, False)

    pu.plot_scatter(p.exec_buy_size**0.5, p.pnl, False)
    pu.plot_scatter(p.exec_sell_size**0.5, p.pnl, False)

    # 投資指標のヒストグラム
    plt.hist(p.exec_buy_size**0.5 - p.exec_sell_size**0.5, bins=int(len(p)**0.5))

    plt.hist( p.exec_buy_size - p.exec_sell_size, bins=int(len(p)**0.5))

    plt.hist(p.pnl, bins=int(len(p)**0.5))

    # 資産曲線 unixtime vs JPY
    asset = [p.pnl.head(n).sum() for n in range(len(p))]
    plt.scatter(p.order_time, asset, 1)    # 最終資産
    print("asset", p.pnl.sum(), "[JPY]")

    # P.F.
    pf = -1 * p[p.pnl>0].pnl.sum() / p[p.pnl<0].pnl.sum()
    print(pf)


#    positions = positions[positions.order_side is not None]
#    assets = [asset(positions, n) for n in range(1, len(positions) + 1)]
#    positions['asset'] = pd.Series(assets, index=positions.index)
#    final_asset(positions)
#
#    fig = plt.figure(figsize=(6, 4), dpi=300)
#    ax = fig.add_subplot(1, 1, 1)
#    ax.set_title('title')
#    ax.set_xlabel('timestamp')
#    ax.set_ylabel('size')
#    #    ax.set_xlim([380000, 381000])
#    ax.set_ylim([-2500, 7500])
#    #    ax.plot((positions['timestamp']-positions['timestamp'].min())/3600, positions['buy_size'],  c='red', label='buy size 15s')
#    #    ax.plot((positions['timestamp']-positions['timestamp'].min())/3600, positions['sell_size'], c='blue', label='sell size 15s')
#    ax.plot(
#        (positions.timestamp - positions.timestamp.min()) / 3600,
#        positions.asset,
#        c='black',
#        label='asset')
#    ax.legend()
#    fig.savefig('C:/workspace/out.png')
#
