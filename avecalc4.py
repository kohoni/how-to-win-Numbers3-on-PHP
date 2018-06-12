import csv
import sqlite3
import numpy as np
from numpy.random import *
from contextlib import closing

#this variable 
numbers=30001
flagc1=1
flagc0=0

update_iprize=("update raw_data set"
               " iprize=(select case when ((lag1 == 0 and lag2 == 0 and lag3 == 0) and"
               " (box1==0 and box2==0 and box3 ==1))then prize"
               " when ((lag1 == 0 and lag2 == 0 and lag3 == 0) and"
               " (box1 <> 0 or box2 <> 0 or box3 <> 1)) then -1"
               " else 0"
               " end from raw_data where id =?)"
               "where id =?"
              )

update_calc1=("update calc set"
              " calclag1=(select avg(iprize) from raw_data) where id = ?"
             )
update_calc2=("update calc set"
              " calclag2=(select avg(iprize) from raw_data) where id = ?"
             )
update_calc3=("update calc set"
              " calclag3=(select avg(iprize) from raw_data) where id = ?"
             )

define_calc1=("update result set"
	          " maxlag1=(select id from calc where calclag1=(select max(calclag1)from calc))"
	          "where id = 1")
define_calc2=("update result set"
	          " maxlag2=(select id from calc where calclag2=(select max(calclag2)from calc))"
	          "where id = 1")
define_calc3=("update result set"
	          " maxlag3=(select id from calc where calclag3=(select max(calclag3)from calc))"
	          "where id = 1")
try:
   #ここでデータベースを入力
   dbname = 'rand.db'

   with closing(sqlite3.connect(dbname)) as conn:
      c = conn.cursor()

   	  #executeでクリエイト
      create_table1 = '''create table if not exists raw_data(id INTEGER PRIMARY KEY AUTOINCREMENT,raw1 INTEGER,raw2 INTEGER,raw3 INTEGER,raw TEXT,boxtext TEXT,box1 INTEGER,box2 INTEGER,box3 INTEGER,boxint INTEGER,prize INTEGER)'''
      c.execute(create_table1)
      create_table2 = '''create table if not exists boxids(id INTEGER PRIMARY KEY AUTOINCREMENT,boxnum INTEGER,boxnum1 INTEGER,boxnum2 INTEGER,boxnum3 INTEGER)'''
      c.execute(create_table2)
      create_table3 = '''create table if not exists flag_data(id INTEGER PRIMARY KEY AUTOINCREMENT,flag1 INTEGER,flag2 INTEGER,flag3 INTEGER,blank1 INTEGER,blank2 INTEGER,blank3 INTEGER,lag1 INTEGER,lag2 INTEGER,lag3 INTEGER,iprize INTEGER)'''
      c.execute(create_table3)
      #210 records per column intends on boxids
      create_table4 = '''create table if not exists result(id INTEGER PRIMARY KEY AUTOINCREMENT,maxlag1 REAL,maxlag2 REAL,maxlag3 REAL,maxblank1 REAL,maxblank2 REAL,maxblank3 REAL)'''
      c.execute(create_table4)
      #30 records per column, outputting thing is average for lag and blank.
      create_table5 = '''create table if not exists calc(id INTEGER PRIMARY KEY AUTOINCREMENT,calclag1 REAL,calclag2 REAL,calclag3 REAL,calcblank1 REAL,calcblank2 REAL,calcblank3 REAL)'''
      c.execute(create_table5)
      i=1
      n=1
      #lag1
      a=0
      for row in range(0,30):
         a+=1
         #i = lag value
         i=a-1
         n=0
         print(i)
         for row in range(1,numbers):
         	i+=1
         	n+=1
         	t=(i,n)
         	update_lag1=("update raw_data set"
                         " lag1=(select blank1"
                         " from raw_data limit 1 offset ?)"
                         "where id =?"
                        )
         	c.execute(update_lag1,t)
         conn.commit()
         ii=0
         for row in range(1,numbers):
         	ii+=1
         	tt=(ii,ii)
         	c.execute(update_iprize,tt)
         conn.commit()
         ttt=(a)
         c.execute(update_calc1,[ttt])
      conn.commit()
      c.execute(define_calc1)
      conn.commit()
      i=0
      n=0
      for row in range(1,numbers):
         i+=1
         n+=1
         tttt=(i,n)
         update_lag1=("update raw_data set"
                      " lag1=(select blank1"
                      " from raw_data limit 1 offset ((select maxlag1 from result where id = 1)+?-1))"
                      "where id =?"
                     )
         c.execute(update_lag1,tttt)
      conn.commit()
      ii=0
      for row in range(1,numbers):
         ii+=1
         tt=(ii,ii)
         c.execute(update_iprize,tt)
      conn.commit()

      #lag2
      conn.close()
except Exception as e:
 raise e