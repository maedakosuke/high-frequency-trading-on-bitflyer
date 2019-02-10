# -*- coding: utf-8 -*-

import util.timeutil as tu
import json
import websocket
import threading
import sys
from util.Sqlite3DatabaseSystemForBitflyer import Sqlite3DatabaseSystemForBitflyer


class BitflyerRealtimeMessenger:
    def __init__(self, product, channel, sqlite3_file_path):
        self.__dbsystem = Sqlite3DatabaseSystemForBitflyer(sqlite3_file_path)
        self.__product = product
        self.__channel = channel

        self.is_error = False  # 0:normal 1:error 
        self.message_time = 0  # 最後にmessageを受信したunixtime
        self.message_count = 0  # messageを受信した回数
        
        self.initialize_websocket()


    def initialize_websocket(self):
        try:
            # set true to debug
            websocket.enableTrace(False)
            print(tu.now_as_text(), self.__channel, 'open websocket connection')
            self.__ws = websocket.WebSocketApp(
                'wss://ws.lightstream.bitflyer.com/json-rpc',
                on_message = self.__on_message,
                on_error = self.__on_error,
                on_close = self.__on_close
            )
            self.__ws.on_open = self.__on_open
            self.is_error = False

        except Exception as e:
            print(tu.now_as_text(), self.__channel, 'initialize_websocket error', e.args[0], file=sys.stderr)
            self.is_error = True


    def __on_message(self, ws, message):
        try:
            self.message_count += 1
            self.message_time = tu.now_as_unixtime()
            print(tu.now_as_text(), self.__channel, '__on_message', self.message_count)

            message_dict = json.loads(message)
            self.__dbsystem.add_message_to_db(message_dict)
                
        except Exception as e:
            print(tu.now_as_text(), self.__channel, '__on_message error', e.args[0], file=sys.stderr)
    
    
    def __on_error(self, ws, error):
        print(tu.now_as_text(), self.__channel, '__on_error', error, file=sys.stderr)
        # クラスのクライアントがis_errorをウォッチしていて
        # websocketのクローズ、threadのストップと再スタートを行う
        self.is_error = True


    def __on_close(self, ws):
        print(tu.now_as_text(), self.__channel, '__on_close')


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
        print(tu.now_as_text(), self.__channel, 'start websocket thread')
        self.__ws_thread = threading.Thread(target=self.__ws.run_forever)
        self.__ws_thread.daemon = True
        self.__ws_thread.start()


    # クラスのクライアントからコールしないとcannot join current threadエラーが出る
    def stop_websocket_thread(self):
        print(tu.now_as_text(), self.__channel, 'stop websocket thread')
        if self.__ws in locals():
            self.__ws.close()
        if self.__ws_thread in locals():
            self.__ws_thread.join()




if __name__ == '__main__':

    product = 'FX_BTC_JPY'
    dbfile_path = 'C:/workspace/test.sqlite3'

    executions_messenger = BitflyerRealtimeMessenger(product, 'lightning_executions_FX_BTC_JPY', dbfile_path)
    executions_messenger.start_websocket_thread()

    ticker_messenger = BitflyerRealtimeMessenger(product, 'lightning_ticker_FX_BTC_JPY', dbfile_path)
    ticker_messenger.start_websocket_thread()

    board_messenger = BitflyerRealtimeMessenger(product, 'lightning_board_FX_BTC_JPY', dbfile_path)
    board_messenger.start_websocket_thread()

    board_ss_messenger = BitflyerRealtimeMessenger(product, 'lightning_board_snapshot_FX_BTC_JPY', dbfile_path)
    board_ss_messenger.start_websocket_thread()

    tu.sleep(10)

    # ssは10秒後にストップする    
    board_ss_messenger.stop_websocket_thread()
    
    while(True):
        if executions_messenger.is_error:
            executions_messenger.stop_websocket_thread()
            dt = tu.now_as_unixtime() - executions_messenger.message_time
            if dt > 10:
                executions_messenger.initialize_websocket()
                executions_messenger.start_websocket_thread()
            
        if ticker_messenger.is_error:
            ticker_messenger.stop_websocket_thread()
            dt = tu.now_as_unixtime() - ticker_messenger.message_time
            if dt > 10:
                ticker_messenger.initialize_websocket()
                ticker_messenger.start_websocket_thread()

        if board_messenger.is_error:
            board_messenger.stop_websocket_thread()
            dt = tu.now_as_unixtime() - board_messenger.message_time
            if dt > 10:
                board_messenger.initialize_websocket()
                board_messenger.start_websocket_thread()
            
        tu.sleep(1)
