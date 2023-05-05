import pandas as pd
import stock_config as sc

db_config = sc.DB_CONFIG

cnx = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'], database=db_config['database'])
cursor = cnx.cursor()

stock_performance = pd.read_sql_query("SELECT * FROM stock_performance", cnx)
stock_forcase = pd.read_sql_query("SELECT * FROM stock_forcase", cnx)

stock_performance.to_csv('业绩报告.csv')
stock_forcase.to_csv('业绩预告.csv')