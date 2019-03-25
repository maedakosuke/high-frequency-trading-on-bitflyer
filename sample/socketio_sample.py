# -*- coding: utf-8 -*-
"""
Created on Sat Mar 16 22:38:42 2019

@author: kosuke

pip install python-socketio
"""

# -*- coding: utf-8 -*-
import socketio

sio = socketio.Client()

def on_connect():
    sio.emit('subscribe','lightning_ticker_FX_BTC_JPY')
    sio.emit('subscribe','lightning_executions_FX_BTC_JPY')
    sio.emit('subscribe','lightning_board_FX_BTC_JPY')

def on_ticker(data):
    print('ticker', len(data))
    print(type(data))
    print(data)

def on_executions(data):
    print('executions', len(data))
    print(type(data))
    print(data)

def on_board(data):
    print('board', len(data))
    print(type(data))
    print(data)

sio.on('connect', on_connect)
#sio.on('lightning_ticker_FX_BTC_JPY', on_ticker)
sio.on('lightning_executions_FX_BTC_JPY', on_executions)
#sio.on('lightning_board_FX_BTC_JPY', on_board)
sio.connect('https://io.lightstream.bitflyer.com',transports=['websocket'])
