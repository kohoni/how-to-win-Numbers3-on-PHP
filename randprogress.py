import csv
import sqlite3
import numpy as np
from numpy.random import *

#These variance generates random numbers 0 to 9 . I use law of large numbers because it gets near to 'The Truth'. 
#これらの変数は0から9までの乱数を生成します。大数の法則を利用しています。なぜなら「ザ・トゥルース」に近づいているからです。
ran1=randint(0,9,30000)
ran2=randint(0,9,30000)
ran3=randint(0,9,30000)
data=np.c_[ran1,ran2,ran3]

np.savetxt('rand.csv',data,delimiter=',')
#with open('rand.csv','w') as f:
   #writer = csv.writer(f,lineterminator='\n')
   #writer.writerow(ran1)
   #writer.writerow(ran2)
   #writer.writerow(ran3)
