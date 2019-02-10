# -*- coding: utf-8 -*-
"""
timeutil.py
Created on Sat Feb  9 10:55:59 2019
@author: kosuke
"""

from datetime import datetime
from pytz import timezone
from time import sleep


# yyyy-mm-dd hh:mm:ss.ffffff形式の文字列をunixtimeに変換する
def time_as_unixtime(time_as_text):
    return datetime.strptime(time_as_text, '%Y-%m-%d %H:%M:%S.%f').timestamp()


# bitFlyerが提供している時刻はUTCだから日本時間JSTより-09:00の時刻になっている
# データベースにはUTCの時刻のままtimestamp()にした値を書き込む
def text_to_unixtime(time_as_text):
    # 'yyyy-mm-ddThh:nn:ss.abcdefgZ' to 'yyyy-mm-dd hh:nn:ss.abcdef'
    time_as_text = time_as_text.replace('T', ' ')
    time_as_text = time_as_text[:-2]
    # convert str to unixtime
    return time_as_unixtime(time_as_text)


# 現在時刻をUTCタイムゾーンのunixtimeで返す    
def now_as_unixtime():
    #return datetime.now('UTC').timestamp()  # timestamp()するとJSTの時刻になっている
    return datetime.utcnow().timestamp()


# 現在時刻をJSTタイムゾーン文字列で返す
def now_as_text(area='Asia/Tokyo'):
    return str(datetime.now(timezone(area)))
    

# unixtime(UTC)をJST時刻形式の文字列に変換する
def time_as_text(unixtime):
    t = datetime.fromtimestamp(unixtime + 9*60*60)
    return str(t.astimezone(timezone('Asia/Tokyo')))



if __name__ == '__main__':

    t1 = time_as_unixtime('2019-01-01 00:00:00.123456')
    print(t1)
    
    # bitflyerの時刻文字列をunixtimeに変換する
    t2 = text_to_unixtime('2019-02-09T17:33:00.1234567Z')
    print(t2)
    
    # 現在時刻をUTCのunixtimeで返す
    t3 = now_as_unixtime()
    print(t3)
    
    # 現在時刻を文字列で返す
    t4 = now_as_text()  # JST
    print(t4)
    t5 = now_as_text('UTC')  # UTC
    print(t5)
