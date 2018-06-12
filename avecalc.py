import csv
import sqlite3
import numpy as np
from numpy.random import *
from contextlib import closing

#this variable 
numbers=30001

with open('flag.csv','w') as f:
   writer = csv.writer(f,lineterminator='\n')

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
      fp=open("box.csv")
      g=csv.reader(fp)
      fp1=open("rand.csv")
      h=csv.reader(fp1)
      i = 0
      #this for-sentence intends on boxids
      for boxing in g:
         #init calc
         for row in h:
            if boxing[1]==row[4] :
               flag1=1
            else :
               flag1=0
            if boxing[2]==row[5] :
               flag2=1
            else :
               flag2=0
            if boxing[3]==row[6] :
               flag3=1
            else :
               flag3=0
            t = (flag1,flag2,flag3)
            c.execute("insert into raw_data (flag1,flag2,flag3) values(?,?,?)",t)
         conn.commit()
         for row in h:
            #laginit blankinit start
            lag1=1
            lag2=1
            lag3=1
            blank1=1
            blank2=1
            blank3=1
            #if文つけ
            tt=(numbers,row)
            insert_table_blank1=("insert into raw_data(blank1) select" 
            	                 "case when flag1 == 1 where id = ? - ? then 1" 
            	                 "when flag1 == 0 then 0"
            	                 "end from raw_data"
            	                )
            c.execute(insert_table_blank1,tt)
            insert_table_blank2="insert inossssss raw_data(blank2) select case flag2 == 1 then 1 from raw_data"
            c.execute(insert_table_blank2)
            insert_table_blank3="insert into raw_data(blank3) select case flag3 == 1 then 1 from raw_data"
            c.execute(insert_table_blank3)
            insert_table_lag1="insert into raw_data (lag1) select blank1 from raw_data limit 1 offset 1"
            c.execute(insert_table_lag1)
            insert_table_lag2="insert into raw_data (lag2) select blank2 from raw_data limit 1 offset 1"
            c.execute(insert_table_lag2)
            insert_table_lag3="insert into raw_data(lag3) select blank3 from raw_data limit 1 offset 1"
            c.execute(insert_table_lag3)
         conn.commit()

         def calc_it():
         	#caution case-case!
         	boxing1=boxing[1]
         	boxing2=boxing[2]
         	boxing3=boxing[3]
         	row1=row[4]
         	row2=row[5]
         	row3=row[6]
         	t1=(boxing1,row1,boxing2,row2,boxing3,row3)
            #caution case-case and else else!
         	calc_average=("insert into raw_data(iprize) select"
         	              "case lag1 == 0 and lag2 == 0 and lag3 then"
         				  "case ? == ? and ? == ? and ? == ? then"
         				  "prize"
         				  "else -1"
         				  "else 0"
         				  "end"
         				  "from raw_data"
            )
            

         
         calcprogress="insert into calc(?) select avg(iprize) from raw data"

         maxdefine="insert into result()" 

         for x in range(0,3):
         	for y1 in range(0,30):
         	   for row in h:
         	      insert_table_lag1="insert into raw_data (lag1) select blank1 from raw_data limit 1 offset ?"
                  c.execute(insert_table_lag1,[y1])
         	   conn.commit()
         	      calc_it().c.execute(calc_average,t1)
               conn.commit()
                  c.execute(calcprogress,["calclag1"])
               conn.commit()



      conn.close()
except Exception as e:
 raise e