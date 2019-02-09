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
    return datetime.now(timezone('UTC')).timestamp()


# 現在時刻をJSTタイムゾーン文字列で返す
def now_as_text():
    return str(datetime.now(timezone('Asia/Tokyo')))
    
    

if __name__ == '__main__':

    t1 = time_as_unixtime('2019-01-01 00:00:00.123456')
    
    # bitflyerの時刻文字列をunixtimeに変換する
    t2 = text_to_unixtime('2019-01-01T00:00:00.1234567Z')
    
    # 現在時刻をUTCのunixtimeで返す
    t3 = now_as_unixtime()
    
    # 現在時刻を文字列で返す
    t4 = now_as_text()