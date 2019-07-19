#!/usr/bin/env python2
import pandas as pd
import MySQLdb
from pandas.io import sql
from sqlalchemy import create_engine, MetaData, orm    

import ConfigParser

import truncate_dm_fact_nh_agent_retain


config = ConfigParser.RawConfigParser()
config.read('/data0/etl/spoon/.kettle/kettle.properties')

host_name=config.get('BI','BI_DB_HOST')
host_port=int(config.get('BI','BI_DB_PORT'))
user_name=config.get('BI','BI_DB_USERNAME')
user_passwd=config.get('BI','BI_DB_PASSWORD2')

#print host_name,host_port,user_name,user_passwd

def intersect(row,a,b):
     return set(row[a].split(',')) & set(row[b].split(','))


def minus(row,a,b):
     return set(row[a].split(',')) - set(row[b].split(','))

def set_len(row,a):
        a_set=set(row[a])
	return len(a_set)

#engine = create_engine("mysql+mysqldb://etl:Scvph_VItZmO@10.0.4.60:3306/fdd_dm")
#mysql_cn = engine.raw_connection() # option-2

mysql_cn = MySQLdb.connect(host=host_name,port=host_port,user=user_name,passwd=user_passwd,db='fdd_dm')

df = pd.read_sql('select date_type, date_time,city_id,biz_type,base_str,stage1_str,stage2_str,stage3_str,stage4_str,stage5_str,stage6_str,stage7_str,stage8_str,stage9_str,stage10_str,stage11_str,stage12_str from fdd_temporary.tmp_dm_fact_nh_agent_retain  ;',con=mysql_cn)


# intersect agent_id str

df['stage1_intersect_str']=df.apply(intersect,a='base_str',b='stage1_str',axis=1)
df['stage2_intersect_str']=df.apply(intersect,a='base_str',b='stage2_str',axis=1)
df['stage3_intersect_str']=df.apply(intersect,a='base_str',b='stage3_str',axis=1)
df['stage4_intersect_str']=df.apply(intersect,a='base_str',b='stage4_str',axis=1)
df['stage5_intersect_str']=df.apply(intersect,a='base_str',b='stage5_str',axis=1)
df['stage6_intersect_str']=df.apply(intersect,a='base_str',b='stage6_str',axis=1)
df['stage7_intersect_str']=df.apply(intersect,a='base_str',b='stage7_str',axis=1)
df['stage8_intersect_str']=df.apply(intersect,a='base_str',b='stage8_str',axis=1)
df['stage9_intersect_str']=df.apply(intersect,a='base_str',b='stage9_str',axis=1)
df['stage10_intersect_str']=df.apply(intersect,a='base_str',b='stage10_str',axis=1)
df['stage11_intersect_str']=df.apply(intersect,a='base_str',b='stage11_str',axis=1)
df['stage12_intersect_str']=df.apply(intersect,a='base_str',b='stage12_str',axis=1)


# intersect agent_id qty

df['base_qty']=df.apply(lambda row:len(row['base_str'].split(',')),axis=1)

df['stage1_intersect_qty']=df.apply(set_len,a='stage1_intersect_str',axis=1)
df['stage2_intersect_qty']=df.apply(set_len,a='stage2_intersect_str',axis=1)
df['stage3_intersect_qty']=df.apply(set_len,a='stage3_intersect_str',axis=1)
df['stage4_intersect_qty']=df.apply(set_len,a='stage4_intersect_str',axis=1)
df['stage5_intersect_qty']=df.apply(set_len,a='stage5_intersect_str',axis=1)
df['stage6_intersect_qty']=df.apply(set_len,a='stage6_intersect_str',axis=1)
df['stage7_intersect_qty']=df.apply(set_len,a='stage7_intersect_str',axis=1)
df['stage8_intersect_qty']=df.apply(set_len,a='stage8_intersect_str',axis=1)
df['stage9_intersect_qty']=df.apply(set_len,a='stage9_intersect_str',axis=1)
df['stage10_intersect_qty']=df.apply(set_len,a='stage10_intersect_str',axis=1)
df['stage11_intersect_qty']=df.apply(set_len,a='stage11_intersect_str',axis=1)
df['stage12_intersect_qty']=df.apply(set_len,a='stage12_intersect_str',axis=1)


# minus agent_id str



df['stage1_minus_str']=df.apply(minus,a='base_str',b='stage1_str',axis=1)
df['stage2_minus_str']=df.apply(minus,a='base_str',b='stage2_str',axis=1)
df['stage3_minus_str']=df.apply(minus,a='base_str',b='stage3_str',axis=1)
df['stage4_minus_str']=df.apply(minus,a='base_str',b='stage4_str',axis=1)
df['stage5_minus_str']=df.apply(minus,a='base_str',b='stage5_str',axis=1)
df['stage6_minus_str']=df.apply(minus,a='base_str',b='stage6_str',axis=1)
df['stage7_minus_str']=df.apply(minus,a='base_str',b='stage7_str',axis=1)
df['stage8_minus_str']=df.apply(minus,a='base_str',b='stage8_str',axis=1)
df['stage9_minus_str']=df.apply(minus,a='base_str',b='stage9_str',axis=1)
df['stage10_minus_str']=df.apply(minus,a='base_str',b='stage10_str',axis=1)
df['stage11_minus_str']=df.apply(minus,a='base_str',b='stage11_str',axis=1)
df['stage12_minus_str']=df.apply(minus,a='base_str',b='stage12_str',axis=1)


#truncate fdd_dm.dm_fact_nh_agent_retain

truncate_dm_fact_nh_agent_retain

# insert  df data  into fdd_dm.dm_fact_nh_agent_retain


df.to_sql('dm_fact_nh_agent_retain',mysql_cn,if_exists='append', flavor='mysql',schema='fdd_dm',chunksize=500)

mysql_cn.close()


