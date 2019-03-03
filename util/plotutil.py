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


def plot_scatter(x, returns, normalize=True):
    """
    :param np.ndarray x: 指標
    :param np.ndarray returns: リターン
    :param bool normalize: x をスケーリングするかどうか
    """
    assert(len(x) == len(returns))
    # 正規化
    x = (x - x.mean()) / x.std() if normalize else x
    # 散布図
    plt.plot(x, returns, 'x')
    # 回帰直線
    reg = np.polyfit(x, returns, 1)
    plt.plot(x, np.poly1d(reg)(x), color='c', linewidth=2)
    # 区間平均値
    plt.plot(*_steps(x, returns), drawstyle='steps-mid', color='r', linewidth=2)

    # 相関係数（情報係数）
    ic = np.corrcoef(x, returns)[0, 1]
    plt.title(f'IC={ic:.3f}, y={reg[0]:.3f}x{reg[1]:+.3f}')
    plt.grid()
    plt.show()