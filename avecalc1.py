import csv
import sqlite3
import numpy as np
from numpy.random import *
from contextlib import closing

#this variable 
numbers=30001

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
      fp1=open("rand.csv")
      h=csv.reader(fp1)
      i = 0
      #this for-sentence intends on boxids
      for row in h:
         i+=1
         number1="0"
         number2="0"
         number3="1"
         if number1 == row[4]:
         	flag_1=1
         else:
         	flag_1=0

         if number2 == row[5]:
         	flag_2=1
         else:
         	flag_2=0

         if number3 == row[6]:
         	flag_3=1
         else:
         	flag_3=0
         t = (flag_1,flag_2,flag_3,i)
         c.execute("update raw_data set flag1=?,flag2=?,flag3=? where id = ?",t)
      conn.commit()
      conn.close()
except Exception as e:
 raise e