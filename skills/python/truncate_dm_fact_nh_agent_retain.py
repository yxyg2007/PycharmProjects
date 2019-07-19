#!/usr/bin/python

import MySQLdb
import ConfigParser


config = ConfigParser.RawConfigParser()
config.read('/data0/etl/spoon/.kettle/kettle.properties')

host_name=config.get('BI','BI_DB_HOST')
host_port=int(config.get('BI','BI_DB_PORT'))
user_name=config.get('BI','BI_DB_USERNAME')
user_passwd=config.get('BI','BI_DB_PASSWORD2')

mydb = MySQLdb.connect(host=host_name,port=host_port,user=user_name,passwd=user_passwd,db='fdd_dm')

cursor = mydb.cursor()

## create truncate query
#cursor.execute('truncate table dm_fact_nh_agent_retain')

# execute truncate query

mydb.commit() 
cursor.close()
mydb.close()
print "Table truncated."

