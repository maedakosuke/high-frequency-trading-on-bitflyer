# -*- coding: utf-8 -*-

import sqlite3
import json
from datetime import datetime


# change str time format
def uniform_my_standard_time_format(time_as_text):
    # 'yyyy-mm-ddThh:nn:ss.abcdefgZ' to 'yyyy-mm-dd hh:nn:ss.abcdef'
    t = time_as_text.replace('T', ' ')
    return t[:-2]
    

# convert str to datetime
def time_as_datetime(time_as_text):
    print(time_as_text)
    return datetime.strptime(time_as_text, '%Y-%m-%d %H:%M:%S.%f')


# output dict recrord to sqlite3 db
def insert_into_execusions_table(record):
    try:
        dbname = 'bitflyer_lightning_realtime_FX_BTC_JPY.sqlite3'
        connection = sqlite3.connect(dbname)
        statement = '''insert into execusions (id, side, price, exec_date, size)
                       values (:id, :side, :price, :exec_date, :size);'''
        connection.cursor().execute(statement, record)
        connection.commit()
        connection.close()
    except sqlite3.Error as e:
        print('sqlite3.Error: ', e.args[0])


def buy0_sell1(text):
    if text == 'BUY':
        return 0
    else:
        return 1


def write_execusions(message):
    message_json = json.loads(message)
    channel = message_json['params']['channel']
    if (channel != 'lightning_executions_FX_BTC_JPY'):
        return
    execusions = message_json['params']['message']
    for execusion in execusions:
        print(str(execusion))
        record = {
            'id': execusion['id'],
            'side': buy0_sell1(execusion['side']),
            'price': execusion['price'],
            'exec_date': time_as_datetime(uniform_my_standard_time_format(execusion['exec_date'])).timestamp(),
            'size': execusion['size']
        }
        insert_into_execusions_table(record)
    

# message to sqlite3
message = '''{"jsonrpc":"2.0","method":"channelMessage","params":{"channel":"lightning_executions_FX_BTC_JPY","message":[{"id":743340674,"side":"BUY","price":405124.0,"size":0.05,"exec_date":"2019-01-20T09:38:58.3441498Z","buy_child_order_acceptance_id":"JRF20190120-093858-642632","sell_child_order_acceptance_id":"JRF20190120-093858-457527"},{"id":743340675,"side":"BUY","price":405150.0,"size":0.05,"exec_date":"2019-01-20T09:38:58.3441498Z","buy_child_order_acceptance_id":"JRF20190120-093858-642632","sell_child_order_acceptance_id":"JRF20190120-093856-784927"},{"id":743340676,"side":"SELL","price":405122.0,"size":0.01,"exec_date":"2019-01-20T09:38:58.359778Z","buy_child_order_acceptance_id":"JRF20190120-093857-222036","sell_child_order_acceptance_id":"JRF20190120-093858-775500"},{"id":743340677,"side":"SELL","price":405122.0,"size":0.01,"exec_date":"2019-01-20T09:38:58.3753993Z","buy_child_order_acceptance_id":"JRF20190120-093857-472307","sell_child_order_acceptance_id":"JRF20190120-093858-388702"},{"id":743340678,"side":"SELL","price":405122.0,"size":0.01,"exec_date":"2019-01-20T09:38:58.3910251Z","buy_child_order_acceptance_id":"JRF20190120-093857-775495","sell_child_order_acceptance_id":"JRF20190120-093858-472309"},{"id":743340679,"side":"SELL","price":405099.0,"size":0.0145454,"exec_date":"2019-01-20T09:38:58.3910251Z","buy_child_order_acceptance_id":"JRF20190120-093857-388700","sell_child_order_acceptance_id":"JRF20190120-093858-506557"},{"id":743340680,"side":"SELL","price":405099.0,"size":0.05,"exec_date":"2019-01-20T09:38:58.4066494Z","buy_child_order_acceptance_id":"JRF20190120-093857-388700","sell_child_order_acceptance_id":"JRF20190120-093858-642634"},{"id":743340681,"side":"SELL","price":405099.0,"size":0.0354546,"exec_date":"2019-01-20T09:38:58.4066494Z","buy_child_order_acceptance_id":"JRF20190120-093857-388700","sell_child_order_acceptance_id":"JRF20190120-093858-222047"},{"id":743340682,"side":"SELL","price":405096.0,"size":0.0145454,"exec_date":"2019-01-20T09:38:58.4222757Z","buy_child_order_acceptance_id":"JRF20190120-093858-661238","sell_child_order_acceptance_id":"JRF20190120-093858-222047"},{"id":743340683,"side":"BUY","price":405123.0,"size":0.01,"exec_date":"2019-01-20T09:38:58.4535212Z","buy_child_order_acceptance_id":"JRF20190120-093858-457528","sell_child_order_acceptance_id":"JRF20190120-093858-877465"},{"id":743340684,"side":"BUY","price":405150.0,"size":0.98,"exec_date":"2019-01-20T09:38:58.4535212Z","buy_child_order_acceptance_id":"JRF20190120-093858-457528","sell_child_order_acceptance_id":"JRF20190120-093856-784927"},{"id":743340685,"side":"BUY","price":405150.0,"size":0.2,"exec_date":"2019-01-20T09:38:58.4691452Z","buy_child_order_acceptance_id":"JRF20190120-093858-588340","sell_child_order_acceptance_id":"JRF20190120-093856-784927"},{"id":743340686,"side":"SELL","price":405096.0,"size":0.0054546,"exec_date":"2019-01-20T09:38:58.4847709Z","buy_child_order_acceptance_id":"JRF20190120-093858-661238","sell_child_order_acceptance_id":"JRF20190120-093858-588341"},{"id":743340687,"side":"SELL","price":405095.0,"size":0.0345454,"exec_date":"2019-01-20T09:38:58.4847709Z","buy_child_order_acceptance_id":"JRF20190120-093857-002083","sell_child_order_acceptance_id":"JRF20190120-093858-588341"}]}}'''
write_execusions(message)



def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def select_execusions_table(id):
    try:
        dbname = 'C:/workspace/bitflyer_lightning_realtime_FX_BTC_JPY.sqlite3'
        conn = sqlite3.connect(dbname)
        conn.row_factory = dict_factory  # use dict, not tupple
        cur = conn.cursor()
        statement = '''select id, exec_date, side, price, size from execusions 
                       where id = :id;'''
        cur.execute(statement, {'id': id})
        res = cur.fetchall()
        conn.close()
        return res
    except sqlite3.Error as e:
        print('sqlite3.Error: ', e.args[0])
    

# get execusion from sqlite3
res = select_execusions_table(743340677)


