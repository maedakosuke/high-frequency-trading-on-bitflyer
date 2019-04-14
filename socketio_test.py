# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 20:45:26 2019

@author: kosuke
"""

import sys
import socketio
import util.timeutil as tu
from util.Sqlite3DatabaseSystemForBitflyer import Sqlite3DatabaseSystemForBitflyer


if len(sys.argv) != 3:
    print('Usage: python bitflyer_socketio.py <sqlite3 file path> <channel name>')
    sys.exit(1)

sqlite3_file_path = sys.argv[1]
channel_name = sys.argv[2]

sio = socketio.Client(
    reconnection_delay=600,
    reconnection_delay_max=600,
    randomization_factor=0.0,
    logger=True)
dbsystem = Sqlite3DatabaseSystemForBitflyer(sqlite3_file_path)


@sio.on('connect')
def on_connect():
    print(tu.now_as_text(), sqlite3_file_path, channel_name, 'connected')


@sio.on('lightning_ticker_FX_BTC_JPY')
def on_ticker(data):
    try:
        if data:
            print(tu.now_as_text(), sqlite3_file_path, channel_name, 'ticker')
            dbsystem.write_ticker(data)
    except TimeoutError as e:
        print(tu.now_as_text(), sqlite3_file_path, channel_name, 'ticker')
        print(e)


@sio.on('lightning_executions_FX_BTC_JPY')
def on_executions(data):
    try:
        if data:
            print(tu.now_as_text(), sqlite3_file_path, channel_name, 'executions')
            dbsystem.write_executions_many(data)
    except TimeoutError as e:
        print(tu.now_as_text(), sqlite3_file_path, channel_name, 'ticker')
        print(e)


@sio.on('lightning_board_FX_BTC_JPY')
def on_board(data):
    try:
        if data:
            print(tu.now_as_text(), sqlite3_file_path, channel_name, 'board')
            dbsystem.write_bids_many(data['bids'])
            dbsystem.write_asks_many(data['asks'])
    except TimeoutError as e:
        print(tu.now_as_text(), sqlite3_file_path, channel_name, 'ticker')
        print(e)


@sio.on('disconnect')
def on_disconnect():
    print(tu.now_as_text(), sqlite3_file_path, channel_name, 'disconnected')


sio.connect('https://io.lightstream.bitflyer.com', transports=['websocket'])
sio.emit('subscribe', channel_name)


while(True):
    tu.sleep(10)

sio.disconnect()
