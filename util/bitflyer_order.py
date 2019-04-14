# -*- coding: utf-8 -*-
"""
Created on Sat Apr 13 15:08:07 2019

@author: kosuke
"""

import pybitflyer
import util.timeutil as tu
import util.cnst as cnst

api = pybitflyer.API(api_key=cnst.API_KEY_BF, api_secret=cnst.API_SECRET_BF)


def order(order_side, order_size = 0.1):
    ret = {}
    ret = api.sendchildorder(
        product_code=cnst.PRODUCT,
        child_order_type='MARKET',
        side=order_side,
        size=order_size,
        minute_to_expire=1)
    print(ret)
    assert(len(ret) > 0)
    return ret


def order_check_position(order_side, first_order_size = 0.1):
    order_size_min = 0.01
    order_size_max = 1.0

    positions = []
    try_count = 0
    while (len(positions) == 0):
        positions = api.getpositions(product_code=cnst.PRODUCT)
        tu.sleep(0.1)
        try_count += 1
        if (try_count > 10):
            return
    print('positions', len(positions))

    buy_position = 0
    sell_position = 0
    for position in positions:
        if position['side'] == 'BUY':
            buy_position += position['size']
        elif position['side'] == 'SELL':
            sell_position += position['size']
    print('buy_positin', buy_position, 'sell_position', sell_position)

    if order_side == 'BUY':
        if buy_position >= first_order_size:
            return
        if sell_position >= first_order_size:
            order_size = round(2 * sell_position, 1)
        else:
            order_size = first_order_size

    elif order_side == 'SELL':
        if sell_position >= first_order_size:
            return
        if buy_position >= first_order_size:
            order_size = round(2 * buy_position, 1)
        else:
            order_size = first_order_size

    if order_size > order_size_max:
        order_size = order_size_max

    print('order', order_side, order_size)

    ret = api.sendchildorder(
        product_code=cnst.PRODUCT,
        child_order_type='MARKET',
        side=order_side,
        size=order_size,
        minute_to_expire=1)
    print(ret)


def close_all_order():
    pass