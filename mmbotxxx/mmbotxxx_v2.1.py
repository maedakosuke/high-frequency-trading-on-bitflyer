# -*- coding: utf-8 -*-

from datetime import datetime
from dateutil.parser import parse
from functools import reduce
import json
import math
from operator import add
import pprint
import pybitflyer
from pytz import timezone
import sys
from time import sleep
import threading
import websocket

###################################################

# parameter
bitflyer_params = {
    'ordersize': 0.01,
    'deltaTime': 5,
    'orderfilter': 1.0,
    'profitspread': 100,
    'orderbreak': 1,
    'loopinterval': 0.1,
}

ok_status =  ['NORMAL','BUSY','VERY BUSY']

# secrets
# Label: exclude_in_and_out_money
# bitFlyer Lightning API
API_KEY_BF = "9KMDohNkN98E3R3PNxpR3A"
API_SECRET_BF = "FHeL+SZp6KgIAgCEnmIPJZIxYuYtjAHf42JZqBxPEWk="

##################################################

PRODUCT = 'FX_BTC_JPY'

volume = {
    'buy': 0,
    'sell': 0,
    'time': datetime.now()
}
executions = {
    'BUY': {},
    'SELL': {}
}

# start RealtimeAPI for volume check
def on_message(ws, message):

    res = json.loads(message)
    global executions, volume

    # (1) store 'side' and 'size' of 'executions'
    if 'method' in res and res['method'] == 'channelMessage':
        for order in res['params']['message']:
            timestamp = parse(order['exec_date']).timestamp()

            if timestamp in executions[order['side']]:
                executions[order['side']][timestamp] += order['size']
            else:
                executions[order['side']][timestamp] = order['size']

    # (2) delete old one
    executions['BUY'] = dict(filter(lambda x: datetime.now().timestamp() - x[0] < bitflyer_params['deltaTime'], executions['BUY'].items()))
    executions['SELL'] = dict(filter(lambda x: datetime.now().timestamp() - x[0] < bitflyer_params['deltaTime'], executions['SELL'].items()))

    # (3) update sum of buy & sel for certain duration
    volume = {
      'buy': reduce(add, executions['BUY'].values(), 0),
      'sell': reduce(add, executions['SELL'].values(), 0),
      'time': datetime.now()
    }

def on_close_and_error(ws, error):
    print(format(error))
    global volume
    volume = {
        'buy': 0,
        'sell': 0,
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

    print('------PARAMETER------')
    pprint.pprint(bitflyer_params)
    print('---------------------')

    global ws, wst
    ws = websockets_bitflyer()
    wst = threading.Thread(target=ws.run_forever)
    wst.daemon = True
    wst.start()

    websockets_bitflyer_monitor()

    api = pybitflyer.API(api_key=API_KEY_BF, api_secret=API_SECRET_BF)

    n = 1
    print('Calculating...')
    print('')
    sleep(bitflyer_params['deltaTime'])
    while True:
        try:

            # Check if Websockets is OK time
            jst_time = datetime.now(timezone('Asia/Tokyo'))
            if jst_time.hour == 3 and jst_time.minute == 59:
                print('loop sleep due to websockets maintenance...')
                sleep(900)

            print('%i . MMBOT %s' % (n, datetime.now()))
            while volume['buy'] == 0 and volume['sell'] == 0:
              sleep(0.1)

            # (1) Check Volume
            buy_volume = volume['buy']
            sell_volume = volume['sell']
            print('%dsec Volume - Buy %f  Sell %f' % (bitflyer_params['deltaTime'], buy_volume, sell_volume))

            # (2) Caluculate Total volume
            difference = math.fabs(math.sqrt(buy_volume) - math.sqrt(sell_volume))
            print('%dsec Volume - Difference %f' % (bitflyer_params['deltaTime'], difference))

            # (3) get exchange status (HTTP public API)
            current_status = api.gethealth(product_code=PRODUCT)['status']

            # (4) get board (HTTP public API)
            bids_and_asks = api.board(product_code=PRODUCT)
            print('Moment Price - Buy %f  Sell %f' % ( bids_and_asks['bids'][0]['price'], bids_and_asks['asks'][0]['price']))

            if bitflyer_params['orderfilter'] < difference and current_status in ok_status:
                order_size = bitflyer_params['ordersize']
                if buy_volume > sell_volume:
                    order_price = bids_and_asks['bids'][0]['price'] - bitflyer_params['profitspread']
                    order_side = 'BUY'
                else:
                    order_price = bids_and_asks['asks'][0]['price'] + bitflyer_params['profitspread']
                    order_side = 'SELL'

                # (5) send a new order (HTTP privte API)
                api.sendchildorder(
                    product_code = PRODUCT,
                    child_order_type = 'LIMIT',
                    side = order_side,
                    price = order_price,
                    size = order_size
                )
                print('%s  Order   - Price %f Size %f' % (order_side, order_price, order_size))

                # (6) cancel all orders
                sleep(bitflyer_params['orderbreak'])
                api.cancelallchildorders(product_code=PRODUCT)
                print('Open Order Canceled\n')
            else:
                print('No Order Posted\n')

            n += 1
            sleep(bitflyer_params['loopinterval'])
        except KeyboardInterrupt:
            print('finish with keyboard interrupt')
            ws.close()
            sys.exit(0)
        except Exception as e:
            print('Main Loop Error - Restart %s' % datetime.now())
            # close connection and reconnect
            reconnect()
