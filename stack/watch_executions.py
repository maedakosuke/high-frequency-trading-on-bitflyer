# -*- coding: utf-8 -*-

from datetime import datetime
import json
from pytz import timezone
import sys
from time import sleep
import threading
import websocket


LOG_FILE_PATH = 'lightning_executions_FX_BTC_JPY.json'

PRODUCT = 'FX_BTC_JPY'

volume = {
    'time': datetime.now()
}


# start RealtimeAPI for volume check
def on_message(ws, message):
    print('%s: got a new message' % datetime.now())
    with open(LOG_FILE_PATH, 'a') as f:
        f.write(message)
        f.write(',\n')

    global volume
    volume = {
      'time': datetime.now()
    }


def on_close_and_error(ws, error):
    print(format(error))
    global volume
    volume = {
        'time': datetime.now()
    }
    ws.close
    sys.exit(1)
    pass


def on_open(ws):
    ws.send(json.dumps(
        {
            'method': 'subscribe',
            'params': { 'channel' : 'lightning_executions_%s' % PRODUCT},
            'id': None
        }
    ))


def websockets_bitflyer():
    try:
        print('WebSocket Connection Open')
        # set true to debug
        websocket.enableTrace(False)
        ws = websocket.WebSocketApp('wss://ws.lightstream.bitflyer.com/json-rpc',
                                    on_message = on_message,
                                    on_error = on_close_and_error,
                                    on_close = on_close_and_error)
        ws.on_open = on_open
        return ws

    except Exception as e:
        print(e.args)
        if ws in locals():
            ws.close


class websockets_bitflyer_monitor():
    def __init__(self, interval=1):
        self.interval = interval
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True
        thread.start()

    def run(self):
        while True:
            print('Checking if WebSocket Alive...')
            now = datetime.now().timestamp()
            sleep(60)
            if now > volume['time'].timestamp() + 60:
                reconnect()


def reconnect():
    global ws, wst
    try:
        ws.close()
        wst.stop()
    except Exception as e:
        pass

    print('WebSocket Connection Error. - Restart %s' % datetime.now())
    sleep(10)
    ws = websockets_bitflyer()
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()


if __name__ == '__main__':
    
    global ws, wst
    ws = websockets_bitflyer()
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()

    websockets_bitflyer_monitor()

    n = 1
    while True:
        try:
            # Check if Websockets is OK time
            jst_time = datetime.now(timezone('Asia/Tokyo'))
            if jst_time.hour == 3 and jst_time.minute == 59:
                print('loop sleep due to websockets maintenance...')
                sleep(900)

            print('%i. %s' % (n, datetime.now()))

            n += 1
            sleep(1)

        except KeyboardInterrupt:
            print('finish with keyboard interrupt')
            ws.close()
            sys.exit(0)

        except Exception as e:
            print('Main Loop Error - Restart %s' % datetime.now())
            # close connection and reconnect
            reconnect()
