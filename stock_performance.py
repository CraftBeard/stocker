import os
os.chdir('/home/project/stocker')

import baostock as bs
import pandas as pd
import pymysql
import sys
import datetime
import stock_config as sc

db_config = sc.DB_CONFIG

cnx = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'], database=db_config['database'])

cursor = cnx.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_performance (
        code varchar(50) comment '证券代码',
        performanceexppubdate date comment '业绩快报披露日',
        performanceexpstatdate date comment '业绩快报统计日期',
        performanceexpupdatedate date comment '业绩快报披露日(最新)',
        performanceexpresstotalasset decimal(20,2) comment '业绩快报总资产',
        performanceexpressnetasset decimal(20,2) comment '业绩快报净资产',
        performanceexpressepschgpct decimal(20,10) comment '业绩每股收益增长率',
        performanceexpressroewa decimal(20,10) comment '业绩快报净资产收益率ROE-加权',
        performanceexpressepsdiluted decimal(20,10) comment '业绩快报每股收益EPS-摊薄',
        performanceexpressgryoy decimal(20,10) comment '业绩快报营业总收入同比',
        performanceexpressopyoy decimal(20,10) comment '业绩快报营业利润同比',
        performanceexpressgryoy decimal(20,10) comment '业绩快报营业总收入同比',
        performanceexpressopyoy decimal(20,10) comment '业绩快报营业利润同比',
        PRIMARY KEY (code, performanceexpstatdate)
    )
""")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_forcast (
        code varchar(50) comment '证券代码',
        profitforcastexppubdate date comment '业绩预告发布日期',
        profitforcastexpstatdate date comment '业绩预告统计日期',
        profitforcasttype varchar(20) comment '业绩预告类型',
        profitforcastabstract varchar(2333) comment '业绩预告摘要',

        profitforcastchgpctup decimal(20,10) comment '预告归属于母公司的净利润增长上限(%)',
        profitforcastchgpctdwn decimal(20,10) comment '预告归属于母公司的净利润增长下限(%)',
        PRIMARY KEY (code, profitforcastexpstatdate)
    )
""")

#### 登陆系统 ####
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

#### 获取公司业绩快报 ####
df_codes = pd.read_sql_query("SELECT * FROM stock_codes", cnx)

df_list = []
for index, row in df_codes.iterrows():

    if index>10: break

    code = row['code']
    name = row['name']

    rs = bs.query_performance_express_report(code, start_date="2015-01-01", end_date="2023-12-31")
    
    result_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        result_list.append(rs.get_row_data())

    for row in result_list:
        code = row[0]
        performanceexppubdate = row[1]
        performanceexpstatdate = row[2]
        performanceexpupdatedate = row[3]
        performanceexpresstotalasset = row[4]
        performanceexpressnetasset = row[5]
        performanceexpressepschgpct = row[6]
        performanceexpressroewa = row[7]
        performanceexpressepsdiluted = row[8]
        performanceexpressgryoy = row[9]
        performanceexpressopyoy = row[10]
        
        # Execute the UPSERT statement for the current row
        cursor.execute("""
            INSERT INTO stock_performance (
                code,
                performanceexppubdate,
                performanceexpstatdate,
                performanceexpupdatedate,
                performanceexpresstotalasset,
                performanceexpressnetasset,
                performanceexpressepschgpct,
                performanceexpressroewa,
                performanceexpressepsdiluted,
                performanceexpressgryoy,
                performanceexpressopyoy
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                performanceexppubdate = VALUES(performanceexppubdate),
                performanceexpstatdate = VALUES(performanceexpstatdate),
                performanceexpupdatedate = VALUES(performanceexpupdatedate),
                performanceexpresstotalasset = VALUES(performanceexpresstotalasset),
                performanceexpressnetasset = VALUES(performanceexpressnetasset),
                performanceexpressepschgpct = VALUES(performanceexpressepschgpct),
                performanceexpressroewa = VALUES(performanceexpressroewa),
                performanceexpressepsdiluted = VALUES(performanceexpressepsdiluted),
                performanceexpressgryoy = VALUES(performanceexpressgryoy),
                performanceexpressopyoy = VALUES(performanceexpressopyoy)
        """, (
            code,
            performanceexppubdate,
            performanceexpstatdate,
            performanceexpupdatedate,
            performanceexpresstotalasset,
            performanceexpressnetasset,
            performanceexpressepschgpct,
            performanceexpressroewa,
            performanceexpressepsdiluted,
            performanceexpressgryoy,
            performanceexpressopyoy
        ))

    rs_forecast = bs.query_forecast_report(code, start_date="2010-01-01", end_date="2023-12-31")
    rs_forecast_list = []
    while (rs_forecast.error_code == '0') & rs_forecast.next():
        # 分页查询，将每页信息合并在一起
        rs_forecast_list.append(rs_forecast.get_row_data())

    for row in rs_forecast_list:
        code = row[0]
        forecastdate = row[1]
        forecasttype = row[2]
        forecastvalue = row[3]
        
        # Execute the UPSERT statement for the current row
        cursor.execute("""
            INSERT INTO stock_forecast (
                code,
                forecastdate,
                forecasttype,
                forecastvalue
            )
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                forecastdate = VALUES(forecastdate),
                forecasttype = VALUES(forecasttype),
                forecastvalue = VALUES(forecastvalue)
        """, (
            code,
            forecastdate,
            forecasttype,
            forecastvalue
        ))

cnx.commit()
cnx.close()

#### 登出系统 ####
bs.logout()