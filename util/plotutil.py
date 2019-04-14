# -*- coding: utf-8 -*-
"""
Created on Sun Mar  3 16:01:30 2019

@author: kosuke
"""

import numpy as np
from matplotlib import pyplot as plt


def _steps(x, y):
    int_x = np.round(x)
    ret_x = np.unique(int_x)
    ret_y = []
    for xa in ret_x:
        ret_y.append(np.average(y[int_x == xa]))
    return ret_x, np.array(ret_y)


def plot_corr(x, returns, normalize=False):
    """
    :param np.ndarray x: 指標
    :param np.ndarray returns: リターン
    :param bool normalize: x をスケーリングするかどうか
    """
    assert(len(x) == len(returns))
    fig = plt.figure(figsize=(5.5,5),dpi=100)
    ax = fig.add_subplot(1,1,1)
    # 正規化
    if normalize:
        x = (x - x.mean()) / x.std()
    # 散布図
    ax.scatter(x, returns, 1)
    # 回帰直線
    reg = np.polyfit(x, returns, 1)
    ax.plot(x, np.poly1d(reg)(x), c='c', linewidth=1)
    # 区間平均値
    #ax.plot(*_steps(x, returns), drawstyle='steps-mid', color='r', linewidth=1)

    # 相関係数（情報係数）
    ic = np.corrcoef(x, returns)[0, 1]
    ax.set_title(f'IC={ic:.3f}, y={reg[0]:.3f}x{reg[1]:+.3f}')
    ax.grid()
#    plt.show()


def plot_1curve(y1, y1_name='y1'):
    fig = plt.figure(figsize=(10,5),dpi=100)
    ax = fig.add_subplot(1,1,1)
    ax.grid()
    ax.set_ylabel(y1_name)
    ax.plot(y1, label=y1_name)


def plot_2curves(y1, y2, y1_name='y1', y2_name='y2'):
    fig = plt.figure(figsize=(10,5),dpi=100)
    ax = fig.add_subplot(1,1,1)
    ax.grid()
    ax.set_ylabel(y1_name)
    ax.plot(y1, label=y1_name)
    ax1 = ax.twinx()
    ax1.plot(y2, c='C1', label=y2_name)
    ax1.set_ylabel(y2_name)


def plot_hist(x):
    fig = plt.figure(figsize=(5.5,5),dpi=100)
    ax = fig.add_subplot(1,1,1)
    ax.hist(x, bins=int(len(x)**0.5))


def plot_live(y1, y2, y3):
    plt.ion()

#    plt.title('')  # グラフタイトル
#    plt.xlabel('x')  # x軸ラベル
#    plt.ylabel('y')  # y軸ラベル
#    plt.subplots_adjust(left=0.1, right=0.95,
#                        bottom=0.1, top=0.95)  # スペース

    plt.figure(1)
    plt.subplot(211)
    plt.cla()  # チャートを初期化
    plt.grid()
    plt.plot(y1, color='blue',
             linewidth = 1.0, linestyle='solid')

    plt.plot(y2, color='magenta',
             linewidth = 1.0, linestyle='solid')

    plt.subplot(212)
    plt.cla()  # チャートを初期化
    plt.grid()
    plt.plot(y3, color='orange',
             linewidth = 1.0, linestyle='solid')


    plt.draw()  # 描画
    plt.pause(0.01)



def plot_depth_live(bids_price, bids_size, asks_price, asks_size, span=10000):
    plt.ion()

#    plt.title('')  # グラフタイトル
#    plt.xlabel('x')  # x軸ラベル
#    plt.ylabel('y')  # y軸ラベル
#    plt.subplots_adjust(left=0.1, right=0.95,
#                        bottom=0.1, top=0.95)  # スペース

    plt.figure(1)
    plt.subplot(111)

    plt.cla()  # チャートを初期化
    plt.grid()
    plt.scatter(bids_price, bids_size, 1, color='green')
    plt.scatter(asks_price, asks_size, 1, color='red')

    ltp = (bids_price.max() + asks_price.min()) / 2
    plt.xlim(ltp - span, ltp + span)
    plt.ylim(0, 500)

    plt.draw()  # 描画
    plt.pause(0.01)