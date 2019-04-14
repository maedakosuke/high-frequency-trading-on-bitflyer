# -*- coding: utf-8 -*-
"""
Created on Sun Mar 31 20:04:50 2019

@author: kosuke

bF executions delay time survey
"""


import plotutil as pu
from BitflyerExchange import BitflyerExchange


dbfile_path = 'C:/workspace/bf20190331.sqlite3'
exchange = BitflyerExchange(dbfile_path)
tmin, tmax = exchange.get_time_range_of_ticker()

executions = exchange.get_executions(tmin, tmax)

delay_time = executions.recieve_time - executions.exec_date
pu.plot_1curve(delay_time)
delay_time.describe()
