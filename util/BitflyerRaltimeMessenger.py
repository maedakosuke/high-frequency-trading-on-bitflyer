# -*- coding: utf-8 -*-

import util.timeutil as tu
import json
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
    # __message_count

    def __init__(self, product, channel, sqlite3_file_path):
        self.__dbsystem = Sqlite3DatabaseSystemForBitflyer(sqlite3_file_path)
        self.__product = product
        self.__channel = channel
        self.__message_count = 0
        
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

#            self.__stop_event = threading.Event()  # 停止させるかのフラグ    

        except Exception as e:
            print('%s __initialize_websocket error: %s' % (self._channel, str(e)))
            if self.__ws in locals():
                self.__ws.close()
        

    # start RealtimeAPI for volume check
    def __on_message(self, ws, message):
#        if self.__stop_event.is_set():
#            return
        try:    
            self.__message_count += 1
            print('%s __on_message %s %s' % (self.__channel, self.__message_count, tu.now_as_text()))
            self.__latest_working_time = tu.now_as_unixtime()

            message_dict = json.loads(message)
            self.__dbsystem.add_message_to_db(message_dict)
                
        except Exception as e:
            print('%s __on_message error: %s' % (self.__channel, str(e)))
    
    
    def __on_error(self, ws, error):
        print('%s __on_error %s' % (self.__channel, error))
        self.__latest_working_time = tu.now_as_unixtime()
#        ws.close()
#        sys.exit(1)


    def __on_close(self, ws):
        print('%s __on_close' % self.__channel)


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
        print('%s start_websocket_thread' % self.__channel)
        self.__ws_thread = threading.Thread(target=self.__ws.run_forever)
        self.__ws_thread.daemon = True
        self.__ws_thread.start()


    def stop_websocket_thread(self):
        print('%s stop_websocket_thread' % self.__channel)
        self.__ws.close()
#        self.__stop_event.set()
        self.__ws_thread.join()


if __name__ == '__main__':

    product = 'FX_BTC_JPY'
    dbfile_path = 'C:/workspace/test.sqlite3'

    execusions_messenger = BitflyerRealtimeMessenger(product, 'lightning_executions_FX_BTC_JPY', dbfile_path)
    execusions_messenger.start_websocket_thread()

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
        tu.sleep(1)
