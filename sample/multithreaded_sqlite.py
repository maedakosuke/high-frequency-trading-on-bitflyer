# -*- coding: utf-8 -*-
"""
Created on Sun Feb  3 08:49:31 2019

@author: https://gist.github.com/User001501/3053f26100ddf281600668fed347e518
"""

from threading import Thread
from queue import Queue
import sqlite3
import sys
import time


class MultiThreadOK(Thread):
    def __init__(self, db):
        super(MultiThreadOK, self).__init__()
        self.db=db
        self.reqs=Queue()
        self.start()

    def run(self):
        cnx = sqlite3.Connection(self.db) 
        cursor = cnx.cursor()
        while True:
            req = self.reqs.get()
            if req=='--close--': 
                break
            elif req=='--commit--': 
                cnx.commit()
            try:
                cursor.executescript(req) if ';' in req else cursor.execute(req)
            except sqlite3.OperationalError as err:
                print('Error {0}'.format(err))
                print('Comando: {0}'.format(req))
            except:
                print("Unexpected error: {0}".format(sys.exc_info()[0]))
        cnx.close()

    def execute(self, req):
        self.reqs.put(req)

    def queries(self):
        return self.reqs.qsize()

    def select(self, req, arg=None):
        cnx = sqlite3.Connection(self.db)
        cursor = cnx.cursor()
        try:
            if results == 0:
                cursor.execute(req)
                ret = [x for x in cursor.fetchall()]
                cnx.close()
            else:
                cursor.execute(req)
                ret = [x for x in cursor.fetchall()[:results]]
                cnx.close()
        except:
            print("Unexpected error: {0}".format(sys.exc_info()[0]))
        finally:
            cnx.close()
        return ret

    def commit(self):
        self.execute("--commit--")

    def close(self):
        self.execute('--close--')


if __name__=='__main__':
    db='people.db'
    sql=MultiThreadOK(db)
    sql.execute("create table people(name,first)")
    sql.execute("insert into people (name,first) values('Czabania','George')")
    sql.commit()
    sql.execute("insert into people (name,first) values('Cooper','Jono')")
    sql.commit()
    sql.execute("insert into people (name,first) values('Wick','John');insert into people values('Anderson','Thomas');")
    sql.commit()
    #much more efficient way to do bulk Inserts and Updates to the DB
    sql.execute("BEGIN TRANSACTION;INSERT INTO people (name,first) values ('Smith','John');INSERT INTO people values ('Anderson','Thomas');COMMIT;")
    for q in sql.select("select * from people"):
        print(q)
    #wait until all write are done to read all updated data
    while sql.queries() > 0:
        time.sleep(5)
    sql.close()
    
    