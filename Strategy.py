# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 17:05:09 2019
@author: kosuke

mmbotxxxのストラテジーを移植する

"""

from BitflyerExchange import BitflyerExchange

class Strategy:
    def __init__(self, sqlite3_file_path):
        self.__exchange = BitflyerExchange(sqlite3_file_path)
        self.params = {}
#        self.position = {}

    # 指値side, price, sizeを決定する
    def decide_order(self):
        pass
    
    def show_now(self):
        global g_now
        print(g_now)
        
    
    def change_time(self):
        global g_now
        g_now = -1 * g_now
    
    

if __name__ == '__main__':
    dbfile_path = 'C:/workspace/test.sqlite3'
    strategy = Strategy(dbfile_path)
    strategy.params = {
        'ordersize': 0.01,
        'deltatime': 5,
        'orderfilter': 1.0,
        'profitspread': 100,
        'orderbreak': 1,
        'loopinterval': 0.1,           
    }

    # global
    g_now = 1.0

    strategy.show_now()
    
    g_now = 2.0
    
    strategy.show_now()
    
    strategy.change_time()

    print(g_now)  
    
    