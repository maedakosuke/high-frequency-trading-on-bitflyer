# -*- coding: utf-8 -*-

import sqlite3
import threading
import sys
import util.cnst as cnst
import util.timeutil as tu
from io import StringIO


def side_as_int(text):
    if text == 'BUY':
        return cnst.BUY
    elif text == 'SELL':
        return cnst.SELL
    else:
        return cnst.ITAYOSE


def dict_factory(cursor, row):
    """
        sqlite3のselectクエリのリターンをdict型にする
    """
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def create_inmemory_sqlit3_connection(db_name):
    """
        sqlite3データベースdb_nameを
        メモリーにロードして接続を返す
    """
    print(
        tu.now_as_text(),
        "start loading sqlite3 :memory: connection from",
        db_name)
    # Read database to tempfile
    tempcon = sqlite3.connect(db_name)
    tempfile = StringIO()
    for line in tempcon.iterdump():
        tempfile.write('%s\n' % line)
    tempcon.close()
    tempfile.seek(0)

    # Create a database in memory and import from tempfile
    memcon = sqlite3.connect(":memory:")
    memcon.cursor().executescript(tempfile.read())
    memcon.commit()
    memcon.row_factory = sqlite3.Row

    print(tu.now_as_text(), "end loading sqlite3 :memory:")

    return memcon


class Sqlite3DatabaseSystemForBitflyer(threading.Thread):
    def __init__(self, db_file_path):
        self.__db_file_path = db_file_path

        # selectクエリ用の常時接続インスタンス
        self.__steady_connection = sqlite3.connect(
            self.__db_file_path, check_same_thread=False)
        self.__steady_connection.row_factory = dict_factory
        self.__steady_cursor = self.__steady_connection.cursor()

        # create tables if not exist
        self.__create_executions_table()
        self.__create_bids_table()
        self.__create_asks_table()
        self.__create_ticker_table()

    # selectクエリを高速化するためにinmemory接続を使用する
    # DBが大きいとロードに時間がかかる
    def use_inmemory_connection_to_select(self):
        self.__steady_connection.close()
        self.__steady_connection = create_inmemory_sqlit3_connection(
                self.__db_file_path)
        self.__steady_connection.row_factory = dict_factory
        self.__steady_cursor = self.__steady_connection.cursor()

    # str statement, dict arg
    def query(self, statement, arg=None):
        try:
            statement = statement.strip()  # 両端の空白を消去する

            if statement.startswith('select') or statement.startswith(
                    'SELECT'):
                # selectクエリは接続のタイムロスをなくすために常時接続しているインスタンスを使用する
                if arg is None:
                    self.__steady_cursor.execute(statement)
                else:
                    self.__steady_cursor.execute(statement, arg)
                return self.__steady_cursor.fetchall()

            else:
                # insert/updateクエリは都度接続したほうがdatabase is lockedエラーが少ない
                tmp_connection = sqlite3.connect(
                    self.__db_file_path, check_same_thread=False)
                tmp_connection.row_factory = dict_factory  # use dict, not tupple
                tmp_cursor = tmp_connection.cursor()
                if arg is None:
                    tmp_cursor.execute(statement)
                else:
                    tmp_cursor.execute(statement, arg)
                tmp_connection.commit()
                tmp_connection.close()

        except sqlite3.Error as e:
            print(
                tu.now_as_text(), 'sqlite3 error', e.args[0], file=sys.stderr)

    def __create_executions_table(self):
        self.query(
            '''
            create table if not exists executions (
                id integer,
                exec_date real,
                side integer,
                price real,
                size real,
                primary key (id, exec_date)
            );
            ''')
        self.query('create index if not exists exec_t on executions (exec_date);')

    def __create_bids_table(self):
        self.query(
            '''
            create table if not exists bids (
                timestamp real,
                price real,
                size real,
                primary key (timestamp, price)
            );

            ''')
        self.query('create index if not exists bids_t on bids (timestamp);')
        self.query('create index if not exists bids_p on bids (price);')
        self.query('create index if not exists bids_tp on bids (timestamp, price);')

    def __create_asks_table(self):
        self.query(
            '''
            create table if not exists asks (
                timestamp real,
                price real,
                size real,
                primary key (timestamp, price)
            );

            ''')
        self.query('create index if not exists asks_t on asks (timestamp);')
        self.query('create index if not exists asks_p on asks (price);')
        self.query('create index if not exists asks_tp on asks (timestamp, price);')

    def __create_ticker_table(self):
        self.query(
            '''
            create table if not exists ticker (
                tick_id integer,
                timestamp real,
                best_bid real,
                best_ask real,
                best_bid_size real,
                best_ask_size real,
                total_bid_depth real,
                total_ask_depth real,
                ltp real,
                volume real,
                volume_by_product real,
                primary key (tick_id, timestamp)
            );
            ''')
        self.query('create index if not exists ticker_t on ticker (timestamp);')

    # dict message
    def add_message_to_db(self, message):
        channel_name = message['params']['channel']  # str
        message_body = message['params']['message']  # dict

        if channel_name == 'lightning_executions_FX_BTC_JPY':
            self.__write_executions(message_body)

        elif channel_name == 'lightning_ticker_FX_BTC_JPY':
            self.__write_ticker(message_body)

        elif channel_name == 'lightning_board_snapshot_FX_BTC_JPY':
            self.__write_orderbook(message_body)

        elif channel_name == 'lightning_board_FX_BTC_JPY':
            self.__write_orderbook(message_body)

        else:
            pass

    # dict executions
    def __write_executions(self, executions):
        statement = '''
            insert into executions
                (id, exec_date, side, price, size)
            values
                (:id, :exec_date, :side, :price, :size)
            on conflict (id, exec_date)
            do update set
                side=excluded.side, price=excluded.price, size=excluded.size;
        '''
        for execution in executions:
            # print(execution)
            record = {
                'id': execution['id'],
                'exec_date': tu.text_to_unixtime(execution['exec_date']),
                'side': side_as_int(execution['side']),
                'price': execution['price'],
                'size': execution['size']
            }
            self.query(statement, record)

    # unixtime t1, t2
    def read_executions_filtered_by_exec_date(self, t1, t2):
        statement = '''
            select id, exec_date, side, price, size from executions
            where exec_date >= :t1 and exec_date <= :t2
            order by id, exec_date;
        '''
        return self.query(statement, {'t1': t1, 't2': t2})

    # str table_name ('bids' or 'asks')
    # dict record
    def __insert_into_bids_or_asks(self, table_name, t, price, size):
        statement = '''
            insert into %s
                (timestamp, price, size)
            values
                (:t, :p, :s)
            on conflict (timestamp, price)
            do update set
                size=excluded.size;
        ''' % table_name
        self.query(statement, {'t': t, 'p': price, 's': size})

    # dict orderbook
    def __write_orderbook(self, orderbook):
        now = tu.now_as_unixtime()
        # orderbook['mid_price']はtickerと重複するので記憶しない
        for bid in orderbook['bids']:
            #            print(bid)
            self.__insert_into_bids_or_asks('bids', now, bid['price'],
                                            bid['size'])

        for ask in orderbook['asks']:
            #            print(ask)
            self.__insert_into_bids_or_asks('asks', now, ask['price'],
                                            ask['size'])

    # 最新のbids(asks)をセレクトして返す
    # str table_name ('bids' or 'asks')
    # str sort ('desc' or 'asc')
    # unixtime t1, t2
    # integer lim
    def __select_from_latest_bids_or_asks(self, table_name, t1, t2, sort, lim):
        # bids/asksテーブルにはスナップショットと差分情報を格納している
        # timestampが新しいレコードを抽出して使うようにすれば最新の板情報となる
        statement = '''
            select timestamp, price, size from %s
            where rowid in (
                select max(rowid) from %s
                where timestamp >= :t1 and timestamp <= :t2
                group by price
                order by timestamp
            ) order by price %s limit :lim;
        ''' % (table_name, table_name, sort)

        return self.query(statement, {'t1': t1, 't2': t2, 'lim': lim})

    # unixtime t1, t2
    def read_latest_bids_filtered_by_timestamp(self, t1, t2, limit=1000):
        # 最高値(best bid)からlimit件までを返す
        return self.__select_from_latest_bids_or_asks('bids', t1, t2, 'desc',
                                                      limit)

    # unixtime t1, t2
    def read_latest_asks_filtered_by_timestamp(self, t1, t2, limit=1000):
        # 最安値(best ask)からlimit件までを返す
        return self.__select_from_latest_bids_or_asks('asks', t1, t2, 'asc',
                                                      limit)

    # dict ticker
    def __write_ticker(self, ticker):
        statement = '''
            insert into ticker
                (tick_id, timestamp, best_bid, best_ask, best_bid_size, best_ask_size, total_bid_depth, total_ask_depth, ltp, volume, volume_by_product)
            values
                (:tick_id, :timestamp, :best_bid, :best_ask, :best_bid_size, :best_ask_size, :total_bid_depth, :total_ask_depth, :ltp, :volume, :volume_by_product);
        '''
        record = {
            'tick_id': ticker['tick_id'],
            'timestamp': tu.text_to_unixtime(ticker['timestamp']),
            'best_bid': ticker['best_bid'],
            'best_ask': ticker['best_ask'],
            'best_bid_size': ticker['best_bid_size'],
            'best_ask_size': ticker['best_ask_size'],
            'total_bid_depth': ticker['total_bid_depth'],
            'total_ask_depth': ticker['total_ask_depth'],
            'ltp': ticker['ltp'],
            'volume': ticker['volume'],
            'volume_by_product': ticker['volume_by_product']
        }
        self.query(statement, record)

    # unixtime t
    def read_latest_ticker(self, t):
        # tick_idを除く全カラム
        statement = '''
            select timestamp, best_bid, best_ask, best_bid_size, best_ask_size, total_bid_depth, total_ask_depth, ltp, volume, volume_by_product
            from ticker
            where timestamp <= :t
            order by timestamp desc
            limit 1;
        '''
        return self.query(statement, {'t': t})

    def read_min_max_timestamp_of_ticker(self):
        statement = '''
            select min(timestamp), max(timestamp) from ticker;
        '''
        return self.query(statement)

    # unixtime t1, t2
    def read_ticker_filtered_by_timestamp(self, t1, t2):
        # tick_idを除く全カラム
        statement = '''
            select timestamp, best_bid, best_ask, best_bid_size, best_ask_size, total_bid_depth, total_ask_depth, ltp, volume, volume_by_product
            from ticker
            where timestamp >= :t1 and timestamp <= :t2
            order by timestamp;
        '''
        return self.query(statement, {'t1': t1, 't2': t2})


if __name__ == '__main__':
    dbfile_path = 'C:/workspace/test.sqlite3'
    dbsystem = Sqlite3DatabaseSystemForBitflyer(dbfile_path)

    import json

    # 約定書き込みテスト
    #    executions_message_sample = '{"jsonrpc":"2.0","method":"channelMessage","params":{"channel":"lightning_executions_FX_BTC_JPY","message":[{"id":743340674,"side":"BUY","price":405124.0,"size":0.05,"exec_date":"2019-01-20T09:38:58.3441498Z","buy_child_order_acceptance_id":"JRF20190120-093858-642632","sell_child_order_acceptance_id":"JRF20190120-093858-457527"},{"id":743340675,"side":"BUY","price":405150.0,"size":0.05,"exec_date":"2019-01-20T09:38:58.3441498Z","buy_child_order_acceptance_id":"JRF20190120-093858-642632","sell_child_order_acceptance_id":"JRF20190120-093856-784927"},{"id":743340676,"side":"SELL","price":405122.0,"size":0.01,"exec_date":"2019-01-20T09:38:58.359778Z","buy_child_order_acceptance_id":"JRF20190120-093857-222036","sell_child_order_acceptance_id":"JRF20190120-093858-775500"},{"id":743340677,"side":"SELL","price":405122.0,"size":0.01,"exec_date":"2019-01-20T09:38:58.3753993Z","buy_child_order_acceptance_id":"JRF20190120-093857-472307","sell_child_order_acceptance_id":"JRF20190120-093858-388702"},{"id":743340678,"side":"SELL","price":405122.0,"size":0.01,"exec_date":"2019-01-20T09:38:58.3910251Z","buy_child_order_acceptance_id":"JRF20190120-093857-775495","sell_child_order_acceptance_id":"JRF20190120-093858-472309"},{"id":743340679,"side":"SELL","price":405099.0,"size":0.0145454,"exec_date":"2019-01-20T09:38:58.3910251Z","buy_child_order_acceptance_id":"JRF20190120-093857-388700","sell_child_order_acceptance_id":"JRF20190120-093858-506557"},{"id":743340680,"side":"SELL","price":405099.0,"size":0.05,"exec_date":"2019-01-20T09:38:58.4066494Z","buy_child_order_acceptance_id":"JRF20190120-093857-388700","sell_child_order_acceptance_id":"JRF20190120-093858-642634"},{"id":743340681,"side":"SELL","price":405099.0,"size":0.0354546,"exec_date":"2019-01-20T09:38:58.4066494Z","buy_child_order_acceptance_id":"JRF20190120-093857-388700","sell_child_order_acceptance_id":"JRF20190120-093858-222047"},{"id":743340682,"side":"SELL","price":405096.0,"size":0.0145454,"exec_date":"2019-01-20T09:38:58.4222757Z","buy_child_order_acceptance_id":"JRF20190120-093858-661238","sell_child_order_acceptance_id":"JRF20190120-093858-222047"},{"id":743340683,"side":"BUY","price":405123.0,"size":0.01,"exec_date":"2019-01-20T09:38:58.4535212Z","buy_child_order_acceptance_id":"JRF20190120-093858-457528","sell_child_order_acceptance_id":"JRF20190120-093858-877465"},{"id":743340684,"side":"BUY","price":405150.0,"size":0.98,"exec_date":"2019-01-20T09:38:58.4535212Z","buy_child_order_acceptance_id":"JRF20190120-093858-457528","sell_child_order_acceptance_id":"JRF20190120-093856-784927"},{"id":743340685,"side":"BUY","price":405150.0,"size":0.2,"exec_date":"2019-01-20T09:38:58.4691452Z","buy_child_order_acceptance_id":"JRF20190120-093858-588340","sell_child_order_acceptance_id":"JRF20190120-093856-784927"},{"id":743340686,"side":"SELL","price":405096.0,"size":0.0054546,"exec_date":"2019-01-20T09:38:58.4847709Z","buy_child_order_acceptance_id":"JRF20190120-093858-661238","sell_child_order_acceptance_id":"JRF20190120-093858-588341"},{"id":743340687,"side":"SELL","price":405095.0,"size":0.0345454,"exec_date":"2019-01-20T09:38:58.4847709Z","buy_child_order_acceptance_id":"JRF20190120-093857-002083","sell_child_order_acceptance_id":"JRF20190120-093858-588341"}]}}'
    #    executions_message_sample = json.loads(executions_message_sample)
    #    dbsystem.add_message_to_db(executions_message_sample)

    # 板情報書き込みテスト
    #    board_ss_sample = '''
    #    {"jsonrpc":"2.0","method":"channelMessage","params":{"channel":"lightning_board_snapshot_FX_BTC_JPY","message":{"mid_price":370433.0,"bids":[{"price":370419.0,"size":1.50195856},{"price":370417.0,"size":0.5},{"price":370415.0,"size":0.01},{"price":370407.0,"size":0.01},{"price":370405.0,"size":0.265},{"price":370404.0,"size":0.2},{"price":370403.0,"size":0.05},{"price":370402.0,"size":0.0354},{"price":370401.0,"size":0.3319},{"price":370400.0,"size":0.53},{"price":370399.0,"size":0.1},{"price":370397.0,"size":0.1},{"price":370396.0,"size":0.18},{"price":370395.0,"size":0.1},{"price":370394.0,"size":0.08},{"price":370393.0,"size":0.89},{"price":370392.0,"size":3.34240459},{"price":370389.0,"size":0.13},{"price":370388.0,"size":0.04},{"price":370387.0,"size":0.03},{"price":370384.0,"size":0.08},{"price":370382.0,"size":0.15149366},{"price":370379.0,"size":0.18599557},{"price":370377.0,"size":0.03},{"price":370376.0,"size":0.89920013},{"price":370375.0,"size":1.5},{"price":370373.0,"size":0.04},{"price":370372.0,"size":0.03},{"price":370371.0,"size":0.02},{"price":370370.0,"size":0.211},{"price":370369.0,"size":0.05},{"price":370368.0,"size":0.07},{"price":370367.0,"size":0.2},{"price":370366.0,"size":0.16},{"price":370365.0,"size":0.021},{"price":370364.0,"size":0.02},{"price":370363.0,"size":0.05},{"price":370362.0,"size":0.2253},{"price":370360.0,"size":0.32985171},{"price":370359.0,"size":0.18},{"price":370358.0,"size":0.01},{"price":370357.0,"size":0.19},{"price":370356.0,"size":0.46},{"price":370355.0,"size":0.11},{"price":370354.0,"size":2.09878051},{"price":370353.0,"size":8.07},{"price":370351.0,"size":0.76868051},{"price":370350.0,"size":1.5},{"price":370349.0,"size":0.59},{"price":370348.0,"size":0.5},{"price":370347.0,"size":0.1027},{"price":370346.0,"size":0.057},{"price":370345.0,"size":0.0184},{"price":370344.0,"size":0.08},{"price":370343.0,"size":0.04},{"price":370342.0,"size":0.21313562},{"price":370341.0,"size":0.18},{"price":370340.0,"size":0.13},{"price":370339.0,"size":0.6},{"price":370337.0,"size":0.015},{"price":370335.0,"size":0.1},{"price":370334.0,"size":0.711},{"price":370333.0,"size":0.07},{"price":370331.0,"size":0.1306},{"price":370329.0,"size":0.04},{"price":370327.0,"size":0.35216307},{"price":370326.0,"size":0.021},{"price":370325.0,"size":0.18},{"price":370324.0,"size":1.01900013},{"price":370323.0,"size":2.0},{"price":370322.0,"size":0.14},{"price":370321.0,"size":0.14},{"price":370320.0,"size":0.13},{"price":370319.0,"size":1.286},{"price":370318.0,"size":0.14},{"price":370317.0,"size":0.021},{"price":370316.0,"size":0.06},{"price":370315.0,"size":0.06},{"price":370314.0,"size":0.03},{"price":370313.0,"size":0.0322},{"price":370312.0,"size":0.1005},{"price":370311.0,"size":0.515},{"price":370310.0,"size":0.02},{"price":370308.0,"size":0.092},{"price":370307.0,"size":0.05},{"price":370305.0,"size":0.1},{"price":370304.0,"size":0.23},{"price":370303.0,"size":0.13},{"price":370302.0,"size":0.44},{"price":370301.0,"size":1.115},{"price":370300.0,"size":4.569},{"price":370299.0,"size":0.16},{"price":370298.0,"size":0.04},{"price":370290.0,"size":0.1},{"price":370289.0,"size":0.0141},{"price":370285.0,"size":0.2877},{"price":370280.0,"size":0.04},{"price":370275.0,"size":0.23},{"price":370273.0,"size":0.03},{"price":370272.0,"size":0.0495},{"price":370270.0,"size":0.13},{"price":370268.0,"size":0.13},{"price":370267.0,"size":2.20000013},{"price":370266.0,"size":1.2},{"price":370265.0,"size":0.02},{"price":370264.0,"size":0.0702},{"price":370262.0,"size":0.41541952},{"price":370261.0,"size":0.39},{"price":370258.0,"size":0.0251},{"price":370257.0,"size":0.8799},{"price":370256.0,"size":5.1564},{"price":370255.0,"size":0.96},{"price":370254.0,"size":1.25641026},{"price":370253.0,"size":0.03},{"price":370252.0,"size":0.15},{"price":370251.0,"size":0.1},{"price":370250.0,"size":7.06102177},{"price":370249.0,"size":0.02},{"price":370248.0,"size":1.0},{"price":370247.0,"size":0.03},{"price":370245.0,"size":0.05},{"price":370244.0,"size":0.06},{"price":370243.0,"size":0.021},{"price":370242.0,"size":0.02},{"price":370240.0,"size":5.0},{"price":370237.0,"size":0.0358},{"price":370233.0,"size":0.043},{"price":370232.0,"size":0.1},{"price":370231.0,"size":0.019},{"price":370230.0,"size":0.213},{"price":370229.0,"size":0.0244},{"price":370226.0,"size":1.0},{"price":370224.0,"size":0.0311},{"price":370223.0,"size":0.05},{"price":370222.0,"size":0.0319},{"price":370219.0,"size":0.075},{"price":370212.0,"size":0.03},{"price":370210.0,"size":1.065},{"price":370209.0,"size":0.052},{"price":370208.0,"size":1.23809524},{"price":370207.0,"size":0.0808},{"price":370206.0,"size":0.2106},{"price":370204.0,"size":0.18},{"price":370203.0,"size":0.08},{"price":370202.0,"size":0.061},{"price":370201.0,"size":0.25},{"price":370200.0,"size":11.08},{"price":370199.0,"size":0.15},{"price":370198.0,"size":2.2},{"price":370194.0,"size":0.01},{"price":370191.0,"size":10.0},{"price":370190.0,"size":0.081},{"price":370187.0,"size":0.2},{"price":370186.0,"size":0.051},{"price":370183.0,"size":0.05},{"price":370182.0,"size":0.15},{"price":370181.0,"size":4.00000013},{"price":370179.0,"size":0.052},{"price":370178.0,"size":0.2257},{"price":370173.0,"size":0.0789},{"price":370171.0,"size":0.13},{"price":370170.0,"size":0.04},{"price":370167.0,"size":0.02},{"price":370166.0,"size":0.115},{"price":370165.0,"size":0.1},{"price":370163.0,"size":0.03},{"price":370162.0,"size":0.2},{"price":370161.0,"size":0.6},{"price":370160.0,"size":1.0},{"price":370159.0,"size":0.95789527},{"price":370158.0,"size":1.521},{"price":370157.0,"size":0.05},{"price":370156.0,"size":0.22},{"price":370155.0,"size":5.33},{"price":370153.0,"size":0.2775},{"price":370152.0,"size":1.6},{"price":370151.0,"size":1.37},{"price":370150.0,"size":0.145},{"price":370148.0,"size":0.5},{"price":370147.0,"size":0.13},{"price":370146.0,"size":0.01},{"price":370145.0,"size":0.1},{"price":370143.0,"size":0.22},{"price":370141.0,"size":0.6862},{"price":370140.0,"size":5.14},{"price":370138.0,"size":0.2442},{"price":370137.0,"size":0.058},{"price":370133.0,"size":0.2},{"price":370132.0,"size":0.22049733},{"price":370131.0,"size":2.0},{"price":370130.0,"size":10.536},{"price":370129.0,"size":0.021},{"price":370127.0,"size":0.0558},{"price":370126.0,"size":0.05},{"price":370125.0,"size":0.0258},{"price":370124.0,"size":0.169},{"price":370123.0,"size":6.0},{"price":370122.0,"size":0.045},{"price":370121.0,"size":0.0632},{"price":370120.0,"size":0.015},{"price":370119.0,"size":0.01},{"price":370118.0,"size":0.301},{"price":370117.0,"size":0.082},{"price":370114.0,"size":0.02},{"price":370113.0,"size":0.4504},{"price":370111.0,"size":1.15},{"price":370110.0,"size":2.8},{"price":370109.0,"size":0.1},{"price":370106.0,"size":0.021},{"price":370105.0,"size":0.036},{"price":370104.0,"size":0.91900757},{"price":370103.0,"size":0.195},{"price":370102.0,"size":0.54100486},{"price":370101.0,"size":5.3},{"price":370100.0,"size":39.1511},{"price":370099.0,"size":1.0},{"price":370098.0,"size":0.1},{"price":370096.0,"size":0.5497},{"price":370094.0,"size":1.36},{"price":370093.0,"size":0.2},{"price":370092.0,"size":0.015},{"price":370090.0,"size":5.05},{"price":370088.0,"size":0.0594},{"price":370087.0,"size":0.06},{"price":370085.0,"size":0.22},{"price":370084.0,"size":0.1397},{"price":370083.0,"size":0.021},{"price":370082.0,"size":0.2},{"price":370080.0,"size":0.52},{"price":370079.0,"size":0.5},{"price":370076.0,"size":1.55},{"price":370074.0,"size":0.2},{"price":370073.0,"size":0.02},{"price":370069.0,"size":0.04},{"price":370067.0,"size":0.3214},{"price":370065.0,"size":0.05},{"price":370064.0,"size":0.019},{"price":370063.0,"size":3.5},{"price":370062.0,"size":0.22},{"price":370061.0,"size":2.74},{"price":370060.0,"size":2.5},{"price":370058.0,"size":0.05},{"price":370057.0,"size":0.04},{"price":370054.0,"size":0.036},{"price":370053.0,"size":3.0},{"price":370051.0,"size":0.16},{"price":370050.0,"size":6.822},{"price":370044.0,"size":0.1},{"price":370042.0,"size":0.03},{"price":370041.0,"size":5.13},{"price":370040.0,"size":0.021},{"price":370039.0,"size":2.23},{"price":370037.0,"size":0.5},{"price":370035.0,"size":0.021},{"price":370034.0,"size":0.1},{"price":370033.0,"size":0.2},{"price":370032.0,"size":0.086},{"price":370031.0,"size":0.062},{"price":370030.0,"size":0.054},{"price":370029.0,"size":6.1},{"price":370027.0,"size":0.02},{"price":370024.0,"size":0.12},{"price":370023.0,"size":0.12},{"price":370022.0,"size":0.51},{"price":370021.0,"size":0.5479},{"price":370020.0,"size":0.5},{"price":370019.0,"size":0.5},{"price":370018.0,"size":0.545},{"price":370017.0,"size":1.24},{"price":370016.0,"size":0.5},{"price":370015.0,"size":0.7},{"price":370014.0,"size":0.6},{"price":370013.0,"size":0.51},{"price":370012.0,"size":0.54},{"price":370011.0,"size":0.63},{"price":370010.0,"size":3.8},{"price":370009.0,"size":0.5},{"price":370008.0,"size":0.65},{"price":370007.0,"size":0.61},{"price":370006.0,"size":5.6},{"price":370005.0,"size":0.8},{"price":370004.0,"size":1.58},{"price":370003.0,"size":1.3},{"price":370002.0,"size":0.8},{"price":370001.0,"size":7.74700686},{"price":370000.0,"size":71.93},{"price":369996.0,"size":0.0225},{"price":369995.0,"size":0.01},{"price":369988.0,"size":1.2},{"price":369986.0,"size":0.32},{"price":369985.0,"size":0.22},{"price":369983.0,"size":11.55},{"price":369980.0,"size":0.53},{"price":369977.0,"size":0.0808},{"price":369970.0,"size":1.0},{"price":369969.0,"size":0.02},{"price":369965.0,"size":1.8},{"price":369964.0,"size":0.021},{"price":369961.0,"size":0.029},{"price":369959.0,"size":0.06}],"asks":[{"price":370448.0,"size":0.3566427},{"price":370449.0,"size":0.33147969},{"price":370450.0,"size":0.03799037},{"price":370451.0,"size":0.07980952},{"price":370452.0,"size":0.48},{"price":370455.0,"size":0.03},{"price":370456.0,"size":0.035},{"price":370461.0,"size":0.13601202},{"price":370462.0,"size":0.04},{"price":370463.0,"size":0.48},{"price":370464.0,"size":0.03799037},{"price":370465.0,"size":0.035},{"price":370466.0,"size":0.06},{"price":370467.0,"size":0.12},{"price":370468.0,"size":0.48245866},{"price":370469.0,"size":0.07578253},{"price":370470.0,"size":18.31540492},{"price":370471.0,"size":5.21937777},{"price":370475.0,"size":0.05},{"price":370478.0,"size":0.35},{"price":370479.0,"size":0.05},{"price":370481.0,"size":0.01},{"price":370484.0,"size":0.29},{"price":370491.0,"size":0.13601202},{"price":370494.0,"size":0.16},{"price":370495.0,"size":0.1},{"price":370496.0,"size":0.03799037},{"price":370498.0,"size":0.04},{"price":370499.0,"size":0.86585853},{"price":370500.0,"size":6.02},{"price":370501.0,"size":0.13601202},{"price":370503.0,"size":0.04},{"price":370505.0,"size":0.01},{"price":370506.0,"size":0.5},{"price":370507.0,"size":0.16},{"price":370508.0,"size":0.01},{"price":370509.0,"size":0.03799037},{"price":370511.0,"size":0.44},{"price":370513.0,"size":0.07707802},{"price":370514.0,"size":0.83},{"price":370515.0,"size":1.00355326},{"price":370517.0,"size":0.02},{"price":370518.0,"size":0.07707802},{"price":370519.0,"size":0.01},{"price":370520.0,"size":0.85},{"price":370521.0,"size":0.639},{"price":370522.0,"size":0.21},{"price":370523.0,"size":0.241},{"price":370524.0,"size":0.0472},{"price":370526.0,"size":0.0129},{"price":370527.0,"size":0.241},{"price":370528.0,"size":0.28},{"price":370531.0,"size":0.14},{"price":370536.0,"size":0.0165},{"price":370537.0,"size":0.1508},{"price":370539.0,"size":0.664},{"price":370540.0,"size":0.02},{"price":370542.0,"size":0.0143},{"price":370545.0,"size":0.186},{"price":370546.0,"size":0.40820976},{"price":370548.0,"size":0.842},{"price":370549.0,"size":1.5},{"price":370550.0,"size":0.021},{"price":370552.0,"size":0.0474},{"price":370553.0,"size":0.5},{"price":370554.0,"size":0.019},{"price":370555.0,"size":0.04},{"price":370556.0,"size":0.04},{"price":370557.0,"size":0.13},{"price":370558.0,"size":0.16},{"price":370559.0,"size":0.13601202},{"price":370560.0,"size":0.05},{"price":370561.0,"size":0.3},{"price":370563.0,"size":0.68878051},{"price":370564.0,"size":0.15},{"price":370565.0,"size":0.4085},{"price":370566.0,"size":0.07598074},{"price":370568.0,"size":0.0482},{"price":370569.0,"size":0.3556497},{"price":370570.0,"size":0.41},{"price":370571.0,"size":0.02},{"price":370572.0,"size":0.88088334},{"price":370573.0,"size":0.02},{"price":370574.0,"size":0.1},{"price":370576.0,"size":0.15},{"price":370577.0,"size":0.6},{"price":370578.0,"size":0.17},{"price":370579.0,"size":0.124},{"price":370580.0,"size":0.7596},{"price":370581.0,"size":0.02},{"price":370584.0,"size":0.1},{"price":370585.0,"size":0.3},{"price":370586.0,"size":0.14},{"price":370587.0,"size":0.47318874},{"price":370588.0,"size":0.0193},{"price":370590.0,"size":0.042},{"price":370591.0,"size":0.18},{"price":370592.0,"size":0.6574},{"price":370593.0,"size":0.2239},{"price":370594.0,"size":0.28},{"price":370596.0,"size":0.01},{"price":370599.0,"size":0.2},{"price":370600.0,"size":3.4213397},{"price":370601.0,"size":0.05},{"price":370604.0,"size":0.5},{"price":370608.0,"size":0.0306},{"price":370609.0,"size":0.035},{"price":370610.0,"size":0.4},{"price":370611.0,"size":0.3743},{"price":370612.0,"size":0.2},{"price":370614.0,"size":0.019},{"price":370616.0,"size":0.5},{"price":370617.0,"size":0.27},{"price":370618.0,"size":0.32},{"price":370619.0,"size":2.0},{"price":370620.0,"size":1.592},{"price":370621.0,"size":0.2},{"price":370622.0,"size":0.43},{"price":370623.0,"size":0.454},{"price":370624.0,"size":0.1},{"price":370625.0,"size":0.22},{"price":370626.0,"size":0.25149366},{"price":370627.0,"size":0.816},{"price":370628.0,"size":0.42},{"price":370630.0,"size":2.02},{"price":370632.0,"size":0.35667269},{"price":370633.0,"size":0.6},{"price":370634.0,"size":1.0},{"price":370635.0,"size":0.02},{"price":370636.0,"size":0.2},{"price":370637.0,"size":0.1279},{"price":370638.0,"size":0.124},{"price":370639.0,"size":0.2369},{"price":370642.0,"size":0.3},{"price":370644.0,"size":0.0101},{"price":370645.0,"size":0.1},{"price":370647.0,"size":0.02},{"price":370648.0,"size":0.02},{"price":370649.0,"size":0.9},{"price":370650.0,"size":11.21506405},{"price":370653.0,"size":0.146},{"price":370654.0,"size":0.05},{"price":370655.0,"size":0.019},{"price":370656.0,"size":0.03},{"price":370657.0,"size":0.08},{"price":370658.0,"size":0.0149},{"price":370660.0,"size":0.1},{"price":370661.0,"size":0.55000013},{"price":370662.0,"size":0.5084},{"price":370664.0,"size":0.13},{"price":370665.0,"size":0.5965},{"price":370666.0,"size":0.31},{"price":370668.0,"size":0.12},{"price":370669.0,"size":0.11},{"price":370670.0,"size":0.56},{"price":370671.0,"size":0.11},{"price":370673.0,"size":0.1},{"price":370676.0,"size":0.042},{"price":370677.0,"size":0.0137},{"price":370678.0,"size":0.066},{"price":370680.0,"size":0.0555},{"price":370685.0,"size":0.21},{"price":370686.0,"size":0.1785},{"price":370690.0,"size":0.4},{"price":370692.0,"size":0.142},{"price":370693.0,"size":0.056},{"price":370694.0,"size":0.619},{"price":370695.0,"size":1.03},{"price":370696.0,"size":0.236},{"price":370698.0,"size":0.2},{"price":370699.0,"size":0.8},{"price":370700.0,"size":30.142},{"price":370702.0,"size":0.13},{"price":370703.0,"size":0.13},{"price":370704.0,"size":0.1},{"price":370705.0,"size":0.05},{"price":370706.0,"size":0.39377757},{"price":370707.0,"size":0.0131},{"price":370714.0,"size":0.036},{"price":370715.0,"size":0.17},{"price":370716.0,"size":0.02},{"price":370717.0,"size":0.0316},{"price":370719.0,"size":0.04},{"price":370721.0,"size":2.65},{"price":370722.0,"size":0.2934},{"price":370723.0,"size":7.015},{"price":370725.0,"size":0.082},{"price":370727.0,"size":0.1749},{"price":370728.0,"size":0.02},{"price":370729.0,"size":0.18},{"price":370730.0,"size":1.0},{"price":370731.0,"size":0.22},{"price":370732.0,"size":0.139},{"price":370734.0,"size":0.285},{"price":370735.0,"size":0.062},{"price":370736.0,"size":0.05},{"price":370737.0,"size":0.1},{"price":370739.0,"size":0.0456},{"price":370740.0,"size":0.222},{"price":370741.0,"size":0.0572},{"price":370743.0,"size":0.04},{"price":370746.0,"size":0.31641952},{"price":370747.0,"size":0.1449},{"price":370748.0,"size":0.3977},{"price":370752.0,"size":0.05},{"price":370753.0,"size":1.00000013},{"price":370754.0,"size":0.5618077},{"price":370755.0,"size":0.81502244},{"price":370756.0,"size":1.1338},{"price":370757.0,"size":0.088},{"price":370759.0,"size":0.9403474},{"price":370760.0,"size":0.22},{"price":370761.0,"size":0.0532},{"price":370762.0,"size":0.08},{"price":370763.0,"size":1.021},{"price":370765.0,"size":0.1},{"price":370766.0,"size":0.1184},{"price":370767.0,"size":1.13668719},{"price":370768.0,"size":0.03},{"price":370770.0,"size":0.142},{"price":370771.0,"size":3.83},{"price":370772.0,"size":0.1911},{"price":370777.0,"size":3.0},{"price":370778.0,"size":0.04},{"price":370780.0,"size":0.5349},{"price":370783.0,"size":0.069},{"price":370786.0,"size":0.482},{"price":370788.0,"size":0.036},{"price":370790.0,"size":0.07789527},{"price":370791.0,"size":0.0781},{"price":370792.0,"size":0.17},{"price":370793.0,"size":0.4},{"price":370794.0,"size":0.3941},{"price":370798.0,"size":0.1},{"price":370800.0,"size":2.45},{"price":370804.0,"size":0.0373},{"price":370805.0,"size":1.0},{"price":370809.0,"size":0.03},{"price":370810.0,"size":0.26},{"price":370811.0,"size":0.05},{"price":370815.0,"size":0.05},{"price":370817.0,"size":0.0485},{"price":370818.0,"size":2.00000013},{"price":370819.0,"size":1.1},{"price":370821.0,"size":3.98},{"price":370822.0,"size":0.03},{"price":370824.0,"size":0.05},{"price":370825.0,"size":0.48542261},{"price":370826.0,"size":0.05},{"price":370829.0,"size":0.1},{"price":370831.0,"size":2.1},{"price":370834.0,"size":0.25},{"price":370835.0,"size":0.03},{"price":370836.0,"size":0.6051},{"price":370839.0,"size":0.4},{"price":370840.0,"size":1.0},{"price":370841.0,"size":0.3215},{"price":370842.0,"size":1.09},{"price":370844.0,"size":1.002},{"price":370846.0,"size":0.045},{"price":370847.0,"size":0.0436},{"price":370850.0,"size":1.65},{"price":370852.0,"size":0.082},{"price":370853.0,"size":0.14},{"price":370854.0,"size":0.672},{"price":370855.0,"size":0.72},{"price":370857.0,"size":0.04},{"price":370858.0,"size":0.0454},{"price":370859.0,"size":8.57693772},{"price":370860.0,"size":0.14837633},{"price":370861.0,"size":0.47},{"price":370863.0,"size":3.0},{"price":370864.0,"size":0.1},{"price":370865.0,"size":0.8},{"price":370866.0,"size":0.208},{"price":370867.0,"size":0.082},{"price":370870.0,"size":0.08},{"price":370871.0,"size":3.13},{"price":370872.0,"size":0.03},{"price":370873.0,"size":0.06},{"price":370874.0,"size":0.086},{"price":370875.0,"size":2.5},{"price":370876.0,"size":0.2219},{"price":370878.0,"size":0.082},{"price":370880.0,"size":0.0851},{"price":370881.0,"size":0.5},{"price":370882.0,"size":0.47},{"price":370884.0,"size":0.082},{"price":370888.0,"size":0.2},{"price":370889.0,"size":0.042},{"price":370890.0,"size":1.3},{"price":370892.0,"size":0.03},{"price":370894.0,"size":1.85},{"price":370895.0,"size":1.53},{"price":370896.0,"size":0.195},{"price":370897.0,"size":0.295},{"price":370898.0,"size":1.05},{"price":370900.0,"size":39.38248143},{"price":370901.0,"size":0.06},{"price":370902.0,"size":0.052}]}}}
    #    '''
    #    board_ss_sample = json.loads(board_ss_sample)
    #    dbsystem.add_message_to_db(board_ss_sample)

    # ティッカー書き込みテスト
    #    ticker_sample = '''
    #    {"jsonrpc":"2.0","method":"channelMessage","params":{"channel":"lightning_ticker_FX_BTC_JPY","message":{"product_code":"FX_BTC_JPY","timestamp":"2019-01-30T12:27:30.9353003Z","tick_id":88060789,"best_bid":372661.0,"best_ask":372663.0,"best_bid_size":0.23,"best_ask_size":0.10,"total_bid_depth":12583.098268080000,"total_ask_depth":11935.427595050000,"ltp":372661.0,"volume":388367.665950170000,"volume_by_product":388367.665950170000}}}
    #    '''
    #    ticker_sample = json.loads(ticker_sample)
    #    dbsystem.add_message_to_db(ticker_sample)

    # 約定履歴読み込みテスト
    t1 = tu.time_as_unixtime('2019-02-01 02:30:00.000000')  # UTC timezone
    t2 = tu.time_as_unixtime('2019-02-10 02:31:00.000000')
    executions_dict = dbsystem.read_executions_filtered_by_exec_date(t1, t2)

    # bids, asks読み込みテスト
    t1 = tu.time_as_unixtime('2019-02-01 10:30:00.000000')  # UTC timezone
    t2 = tu.time_as_unixtime('2019-02-10 14:00:00.000000')
    bids_dict = dbsystem.read_latest_bids_filtered_by_timestamp(
        0, t2 - 9 * 60 * 60)
    asks_dict = dbsystem.read_latest_asks_filtered_by_timestamp(
        0, t2 - 9 * 60 * 60)

    # best_bid, best_ask読み込みテスト
    t = tu.time_as_unixtime('2019-02-10 10:30:00.000000')  # UTC timezone
    ticker_dict = dbsystem.read_latest_ticker(t)

    # ティッカーのデータ所持時刻
    tminmax = dbsystem.read_min_max_timestamp_of_ticker()
