import os
os.chdir('/home/project/stocker')

import baostock as bs
import pandas as pd
import pymysql
import sys
import datetime
import stock_config as sc
import time

db_config = sc.DB_CONFIG
QPS = 10

cnx = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'], database=db_config['database'])

cursor = cnx.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_performance (
        code varchar(50) comment '证券代码',
        pubdate date comment '业绩快报披露日',
        statdate date comment '业绩快报统计日期',
        updatedate date comment '业绩快报披露日(最新)',
        resstotalasset float comment '业绩快报总资产',
        ressnetasset float comment '业绩快报净资产',
        ressepschgpct float comment '业绩每股收益增长率',
        ressroewa float comment '业绩快报净资产收益率ROE-加权',
        ressepsdiluted float comment '业绩快报每股收益EPS-摊薄',
        ressgryoy float comment '业绩快报营业总收入同比',
        ressopyoy float comment '业绩快报营业利润同比',
        PRIMARY KEY (code, statdate)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_forcast (
        code varchar(50) comment '证券代码',
        exppubdate date comment '业绩预告发布日期',
        expstatdate date comment '业绩预告统计日期',
        type varchar(20) comment '业绩预告类型',
        abstract varchar(2333) comment '业绩预告摘要',
        chgpctup float comment '预告归属于母公司的净利润增长上限(%)',
        chgpctdwn float comment '预告归属于母公司的净利润增长下限(%)',
        PRIMARY KEY (code, expstatdate)
    )
""")

#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

#### 获取公司业绩快报 ####
df_codes = pd.read_sql_query("SELECT * FROM stock_codes WHERE LOWER(code) LIKE 'sh.600%' OR code LIKE 'sh.601%' OR code LIKE 'sz.000%'", cnx)

df_list = []
for index, row in df_codes.iterrows():

    code = row['code']
    name = row['code_name']

    print(code, name)

    rs = bs.query_performance_express_report(code, start_date="2015-01-01", end_date="2023-12-31")
    
    result_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        result_list.append(rs.get_row_data())

    print(len(result_list))

    for row in result_list:
        code = row[0].strip()
        pubdate = datetime.datetime.strptime(row[1], '%Y-%m-%d').date()
        statdate = datetime.datetime.strptime(row[2], '%Y-%m-%d').date()
        updatedate = datetime.datetime.strptime(row[3], '%Y-%m-%d').date()
        resstotalasset = float(row[4])
        ressnetasset = float(row[5])
        ressepschgpct = float(row[6])
        ressroewa = float(row[7])
        ressepsdiluted = float(row[8])
        ressgryoy = float(row[9])
        ressopyoy = float(row[10])
        print(row)

        # Execute the UPSERT statement for the current row
        cursor.execute("""
            INSERT INTO stock_performance (
                code,
                pubdate,
                statdate,
                updatedate,
                resstotalasset,
                ressnetasset,
                ressepschgpct,
                ressroewa,
                ressepsdiluted,
                ressgryoy,
                ressopyoy
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                pubdate = VALUES(pubdate),
                updatedate = VALUES(updatedate),
                resstotalasset = VALUES(resstotalasset),
                ressnetasset = VALUES(ressnetasset),
                ressepschgpct = VALUES(ressepschgpct),
                ressroewa = VALUES(ressroewa),
                ressepsdiluted = VALUES(ressepsdiluted),
                ressgryoy = VALUES(ressgryoy),
                ressopyoy = VALUES(ressopyoy)
        """, (
            code,
            pubdate,
            statdate,
            updatedate,
            resstotalasset,
            ressnetasset,
            ressepschgpct,
            ressroewa,
            ressepsdiluted,
            ressgryoy,
            ressopyoy
        ))
    cnx.commit()

    rs_forecast = bs.query_forecast_report(code, start_date="2010-01-01", end_date="2023-12-31")
    rs_forecast_list = []
    while (rs_forecast.error_code == '0') & rs_forecast.next():
        # 分页查询，将每页信息合并在一起
        rs_forecast_list.append(rs_forecast.get_row_data())
    print(len(rs_forecast_list))

    for row in rs_forecast_list:
        code = row[0].strip()
        exppubdate = datetime.datetime.strptime(row[1], '%Y-%m-%d')
        expstatdate = datetime.datetime.strptime(row[2], '%Y-%m-%d')
        type = row[3].strip()
        abstract = row[4].strip()
        chgpctup = float(row[5])
        chgpctdwn = float(row[6])

        print(row)

        # Execute the UPSERT statement for the current row
        cursor.execute("""
            INSERT INTO stock_forcast (
                code,
                exppubdate,
                expstatdate,
                type,
                abstract,
                chgpctup,
                chgpctdwn,
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                exppubdate = VALUES(exppubdate),
                type = VALUES(type),
                abstract = VALUES(abstract),
                chgpctup = VALUES(chgpctup),
                chgpctdwn = VALUES(chgpctdwn)
        """, (
            code,
            exppubdate,
            expstatdate,
            type,
            abstract,
            chgpctup,
            chgpctdwn
        ))
    cnx.commit()

    time.sleep(QPS/60)
    
cnx.close()

#### 登出系统 ####
bs.logout()