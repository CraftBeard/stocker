import os
os.chdir('/home/project/stocker')

import baostock as bs
import pymysql
import sys
import datetime
import stock_config as sc

db_config = sc.DB_CONFIG

if len(sys.argv) > 1:
    input_day = sys.argv[1]
else:
    input_day = datetime.datetime.today().strftime('%Y-%m-%d')

cnx = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'], database=db_config['database'])

cursor = cnx.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_codes (
        code VARCHAR(50) PRIMARY KEY, 
        tradestatus VARCHAR(10), 
        code_name VARCHAR(100)
    )
""")
lg = bs.login()
rs = bs.query_all_stock(day=input_day)
while (rs.error_code == '0') & rs.next():
    data = rs.get_row_data()
    cursor.execute("""
        INSERT INTO stock_codes (code, tradestatus, code_name) 
        VALUES (%s, %s, %s) 
        ON DUPLICATE KEY UPDATE tradestatus=VALUES(tradestatus), code_name=VALUES(code_name)
    """, (data[0], data[1], data[2]))
cnx.commit()
cursor.close()
cnx.close()
bs.logout()
