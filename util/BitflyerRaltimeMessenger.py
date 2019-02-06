# -*- coding: utf-8 -*-

from datetime import datetime
import json
from pytz import timezone
import sys
from time import sleep

import websocket
import threading

from util.Sqlite3DatabaseSystemForBitflyer import Sqlite3DatabaseSystemForBitflyer


class BitflyerRealtimeMessenger:
    # member variables
    # __dbsystem
    # __product
    # __channel
    # __ws
    # __latest_working_time

    def __init__(self, product, channel, sqlite3_file_path):
        self.__dbsystem = Sqlite3DatabaseSystemForBitflyer(sqlite3_file_path)
        self.__product = product
        self.__channel = channel
        
        self.__initialize_websocket()


    def __initialize_websocket(self):
        try:
            # set true to debug
            websocket.enableTrace(False)
            print('WebSocket Connection Open')
            self.__ws = websocket.WebSocketApp(
                'wss://ws.lightstream.bitflyer.com/json-rpc',
                on_message = self.__on_message,
                on_error = self.__on_error,
                on_close = self.__on_close
            )
            self.__ws.on_open = self.__on_open
    
        except Exception as e:
            print(e.args)
            if self.__ws in locals():
                self.__ws.close
        

    # start RealtimeAPI for volume check
    def __on_message(self, ws, message):
        try:
            print('on_message() called: %s' % datetime.now())
            self.__latest_working_time = datetime.now()
            message_dict = json.loads(message)
            self.__dbsystem.add_message_to_db(message_dict)
        except Exception as e:
            print(e.args)
    
    
    def __on_error(self, ws, error):
        print('on_error called')
        print(error)
        self.__latest_working_time = datetime.now()
#        ws.close
#        sys.exit(1)


    def __on_close(self, ws):
        print('close websocket connection')


    def __on_open(self, ws):
        ws.send(
            json.dumps(
                {
                    'method': 'subscribe',
                    'params': {'channel': self.__channel},
                    'id': None
                }
            )
        )
    
    
    def start_websocket_thread(self):
        print('start_websocket_thread() called')
        self.__ws_thread = threading.Thread(target=self.__ws.run_forever)
        self.__ws_thread.daemon = True
        self.__ws_thread.start()


    def stop_websocket_thread(self):
        self.__ws.close
        self.__ws_thread._stop()


if __name__ == '__main__':
    product = 'FX_BTC_JPY'
    dbfile_path = 'C:/workspace/test.sqlite3'
    execusions_messenger = BitflyerRealtimeMessenger(product, 'lightning_executions_FX_BTC_JPY', dbfile_path)
    execusions_messenger.start_websocket_thread()
    ticker_messenger = BitflyerRealtimeMessenger(product, 'lightning_ticker_FX_BTC_JPY', dbfile_path)
    ticker_messenger.start_websocket_thread()
    board_messenger = BitflyerRealtimeMessenger(product, 'lightning_board_FX_BTC_JPY', dbfile_path)
    board_messenger.start_websocket_thread()
#    board_ss_messenger = BitflyerRealtimeMessenger(product, 'lightning_board_snapshot_FX_BTC_JPY', dbfile_path)
#    board_ss_messenger.start_websocket_thread()

#    ticker_messenger.stop_websocket_thread()
    
    while(True):
        sleep(1)
        
