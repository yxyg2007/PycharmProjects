#! /usr/bin/env python
#coding=utf-8

#导入模块
import MySQLdb
#联接数据库
#conn= MySQLdb.connect(host='localhost',user='read_only',passwd='8tXeh_13f7p0',db='fdd_stage_nh',port=13306)
try:
    conn= MySQLdb.connect(host='localhost',user='read_only',passwd='8tXeh_13f7p0',db='fdd_stage_nh',port=13306)
except:
   print "Could not connect to MySQL server."
   exit( 0 )