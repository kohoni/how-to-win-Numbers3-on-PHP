import csv
import sqlite3
import numpy as np
from numpy.random import *
from contextlib import closing

#this variable 
numbers=30001
flagc1=1
flagc0=0

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
      i = 0
      #this for-sentence intends on boxids
      for row in range(1,numbers):
         i+=1
         t=(i,i,i,i)
         update_blank=("update raw_data set"
         	            " blank1=(select case when flag1 == 1 then 1"
         	            " else 0"
         	            " end from raw_data where id =?),"
         	            " blank2=(select case when flag2 == 1 then 1"
         	            " else 0"
         	            " end from raw_data where id =?),"
         	            " blank3=(select case when flag3 == 1 then 1"
         	            " else 0"
         	            " end from raw_data where id =?)"
         	            "where id =?"
         	           )
         c.execute(update_blank,t)
      conn.commit()
      ii=0
      for row1 in range(1,numbers):
         ii+=1
         t=(ii,ii,ii,ii)
         update_lag=("update raw_data set"
         	         " lag1=(select blank1"
         	         " from raw_data limit 1 offset ?),"
         	         " lag2=(select blank2"
         	         " from raw_data limit 1 offset ?),"
         	         " lag3=(select blank3"
         	         " from raw_data limit 1 offset ?)"
         	         "where id =?"
         	        )
         c.execute(update_lag,t)
      conn.commit()
      conn.close()
except Exception as e:
 raise e