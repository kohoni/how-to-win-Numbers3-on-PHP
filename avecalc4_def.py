import csv
import sqlite3
import numpy as np
from numpy.random import *
from contextlib import closing

#this variable 
numbers=30001

#lag1

def function_lag1():

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

def definelag1():
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

def function_lag2():

   i=1
   n=1
   #lag2
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
         update_lag2=("update raw_data set"
                      " lag2=(select blank2"
                      " from raw_data limit 1 offset ?)"
                      "where id =?"
                      )
         c.execute(update_lag2,t)
      conn.commit()
      ii=0
      for row in range(1,numbers):
         ii+=1
         tt=(ii,ii)
         c.execute(update_iprize,tt)
      conn.commit()
      ttt=(a)
      c.execute(update_calc2,[ttt])
   conn.commit()
   c.execute(define_calc2)
   conn.commit()

def definelag2():
   i=0
   n=0
   for row in range(1,numbers):
      i+=1
      n+=1
      tttt=(i,n)
      update_lag2=("update raw_data set"
                   " lag2=(select blank2"
                   " from raw_data limit 1 offset ((select maxlag2 from result where id = 1)+?-1))"
                   "where id =?"
                  )
      c.execute(update_lag2,tttt)
   conn.commit()
   ii=0
   for row in range(1,numbers):
      ii+=1
      tt=(ii,ii)
      c.execute(update_iprize,tt)
   conn.commit()

#lag3

def function_lag3():

   i=1
   n=1
   #lag3
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
         update_lag3=("update raw_data set"
                      " lag3=(select blank3"
                      " from raw_data limit 1 offset ?)"
                      "where id =?"
                      )
         c.execute(update_lag3,t)
      conn.commit()
      ii=0
      for row in range(1,numbers):
         ii+=1
         tt=(ii,ii)
         c.execute(update_iprize,tt)
      conn.commit()
      ttt=(a)
      c.execute(update_calc3,[ttt])
   conn.commit()
   c.execute(define_calc3)
   conn.commit()

def definelag3():
   i=0
   n=0
   for row in range(1,numbers):
      i+=1
      n+=1
      tttt=(i,n)
      update_lag3=("update raw_data set"
                   " lag3=(select blank3"
                   " from raw_data limit 1 offset ((select maxlag3 from result where id = 1)+?-1))"
                   "where id =?"
                  )
      c.execute(update_lag3,tttt)
   conn.commit()
   ii=0
   for row in range(1,numbers):
      ii+=1
      tt=(ii,ii)
      c.execute(update_iprize,tt)
   conn.commit()

#blank1

def function_blank1():

   i=1
   n=1
   #lag3
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
         t=(a,numbers,n,numbers,n,numbers,n,numbers,n,numbers,n)
         update_blank1=("update raw_data set"
                        " blank1=(select case when flag1 == 1 then ?"
                        " when (flag1 <> 1) and ((select blank1 from raw_data where id = ?+1-?)==0) then 0"
                        " when (flag1 <> 1) and ((select blank1 from raw_data where id = ?+1-?) > 0) then (select blank1 from raw_data where id = ?+1-?)-1"
                        " end from raw_data where id =?-?)"
                        "where id = ?-?"
                       )
         c.execute(update_blank1,t)
      conn.commit()
      definelag1()
      ii=0
      for row in range(1,numbers):
         ii+=1
         tt=(ii,ii)
         c.execute(update_iprize,tt)
      conn.commit()
      ttt=(a)
      c.execute(update_calc4,[ttt])
   conn.commit()
   c.execute(define_calc4)
   conn.commit()

def defineblank1():
   i=0
   n=0
   for row in range(1,numbers):
      i+=1
      n+=1
      t=(numbers,n,numbers,n,numbers,n,numbers,n,numbers,n)
      update_blank1=("update raw_data set"
                     " blank1=(select case when flag1 == 1 then (select maxblank1 from result where id = 1)"
                     " when (flag1 <> 1) and ((select blank1 from raw_data where id = ?+1-?)==0) then 0"
                     " when (flag1 <> 1) and ((select blank1 from raw_data where id = ?+1-?) > 0) then (select blank1 from raw_data where id = ?+1-?)-1"
                     " end from raw_data where id =?-?)"
                     "where id = ?-?"
                    )
      c.execute(update_blank1,t)
   conn.commit()
   definelag1()
   ii=0
   for row in range(1,numbers):
      ii+=1
      tt=(ii,ii)
      c.execute(update_iprize,tt)
   conn.commit()

#blank2

def function_blank2():

   i=1
   n=1
   #lag3
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
         t=(a,numbers,n,numbers,n,numbers,n,numbers,n,numbers,n)
         update_blank2=("update raw_data set"
                        " blank2=(select case when flag1 == 1 then ?"
                        " when (flag1 <> 1) and ((select blank2 from raw_data where id = ?+1-?)==0) then 0"
                        " when (flag1 <> 1) and ((select blank2 from raw_data where id = ?+1-?) > 0) then (select blank2 from raw_data where id = ?+1-?)-1"
                        " end from raw_data where id =?-?)"
                        "where id = ?-?"
                       )
         c.execute(update_blank2,t)
      conn.commit()
      definelag2()
      ii=0
      for row in range(1,numbers):
         ii+=1
         tt=(ii,ii)
         c.execute(update_iprize,tt)
      conn.commit()
      ttt=(a)
      c.execute(update_calc5,[ttt])
   conn.commit()
   c.execute(define_calc5)
   conn.commit()

def defineblank2():
   i=0
   n=0
   for row in range(1,numbers):
      i+=1
      n+=1
      t=(numbers,n,numbers,n,numbers,n,numbers,n,numbers,n)
      update_blank2=("update raw_data set"
                     " blank2=(select case when flag1 == 1 then (select maxblank2 from result where id = 1)"
                     " when (flag1 <> 1) and ((select blank2 from raw_data where id = ?+1-?)==0) then 0"
                     " when (flag1 <> 1) and ((select blank2 from raw_data where id = ?+1-?) > 0) then (select blank2 from raw_data where id = ?+1-?)-1"
                     " end from raw_data where id =?-?)"
                     "where id = ?-?"
                    )
      c.execute(update_blank2,t)
   conn.commit()
   definelag2()
   ii=0
   for row in range(1,numbers):
      ii+=1
      tt=(ii,ii)
      c.execute(update_iprize,tt)
   conn.commit()

#blank3

def function_blank3():

   i=1
   n=1
   #lag3
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
         t=(a,numbers,n,numbers,n,numbers,n,numbers,n,numbers,n)
         update_blank3=("update raw_data set"
                        " blank3=(select case when flag1 == 1 then ?"
                        " when (flag1 <> 1) and ((select blank3 from raw_data where id = ?+1-?)==0) then 0"
                        " when (flag1 <> 1) and ((select blank3 from raw_data where id = ?+1-?) > 0) then (select blank3 from raw_data where id = ?+1-?)-1"
                        " end from raw_data where id =?-?)"
                        "where id = ?-?"
                       )
         c.execute(update_blank3,t)
      conn.commit()
      definelag3()
      ii=0
      for row in range(1,numbers):
         ii+=1
         tt=(ii,ii)
         c.execute(update_iprize,tt)
      conn.commit()
      ttt=(a)
      c.execute(update_calc6,[ttt])
   conn.commit()
   c.execute(define_calc6)
   conn.commit()

def defineblank3():
   i=0
   n=0
   for row in range(1,numbers):
      i+=1
      n+=1
      t=(numbers,n,numbers,n,numbers,n,numbers,n,numbers,n)
      update_blank3=("update raw_data set"
                     " blank3=(select case when flag1 == 1 then (select maxblank3 from result where id = 1)"
                     " when (flag1 <> 1) and ((select blank3 from raw_data where id = ?+1-?)==0) then 0"
                     " when (flag1 <> 1) and ((select blank3 from raw_data where id = ?+1-?) > 0) then (select blank3 from raw_data where id = ?+1-?)-1"
                     " end from raw_data where id =?-?)"
                     "where id = ?-?"
                    )
      c.execute(update_blank3,t)
   conn.commit()
   definelag3()
   ii=0
   for row in range(1,numbers):
      ii+=1
      tt=(ii,ii)
      c.execute(update_iprize,tt)
   conn.commit()

################################################################################################################

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
update_calc4=("update calc set"
              " calcblank1=(select avg(iprize) from raw_data) where id = ?"
             )
update_calc5=("update calc set"
              " calcblank2=(select avg(iprize) from raw_data) where id = ?"
             )
update_calc6=("update calc set"
              " calcblank3=(select avg(iprize) from raw_data) where id = ?"
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
define_calc4=("update result set"
	          " maxblank1=(select id from calc where calcblank1=(select max(calcblank1)from calc))"
	          "where id = 1")
define_calc5=("update result set"
	          " maxblank2=(select id from calc where calcblank2=(select max(calcblank2)from calc))"
	          "where id = 1")
define_calc6=("update result set"
	          " maxblank3=(select id from calc where calcblank3=(select max(calcblank3)from calc))"
	          "where id = 1")


################################################################################################################

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

      function_lag1()
      definelag1()
      function_lag2()
      definelag2()
      function_lag3()
      definelag3()
      function_blank1()
      defineblank1()
      function_blank2()
      defineblank2()
      function_blank3()
      defineblank3()

      #lag2
      conn.close()
except Exception as e:
 raise e