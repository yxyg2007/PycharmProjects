#! /usr/bin/env python
#coding=utf-8
from connecttomysql import *

cursor =conn.cursor()
#执行中间操作
mytest='select truename,cellphone from fdd_agent limit 10'
cursor.execute(mytest)

row=cursor.fetchone()
print row

#关闭联接
cursor.close()
conn.close()

