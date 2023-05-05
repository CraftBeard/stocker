import pandas as pd
import stock_config as sc
import pymysql

db_config = sc.DB_CONFIG

cnx = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'], database=db_config['database'])
cursor = cnx.cursor()

stock_performance = pd.read_sql_query("SELECT * FROM stock_performance", cnx)
stock_performance.rename(columns={
    "report_date": "报告日期",
    "code": "证券代码",
    "name": "证券简称",
    "holder_net_income": "归属母公司所有者净利润(元)",
    "operate_income": "营业收入(元)",
    "operate_profit": "营业利润(元)",
    "operate_cost": "营业成本(元)",
    "operate_share_flow": "每股经营现金流量(元)",
    "share_net_asset": "每股净资产(元)",
    "share_earn": "每股收益(元)",
    "roe": "净资产收益率%",
    "operate_margin": "营业利润率%",
    "gross_profit_margin": "毛利率%",
}, inplace=True)
stock_forcast = pd.read_sql_query("SELECT * FROM stock_forcast", cnx)
stock_forcast.rename(columns={
    "report_date": "报告日期",
    "code": "证券代码",
    "name": "证券简称",
    "industry_2nd": "二级行业",
    "perform_type": "业绩变动类型",
    "forcast_content": "业绩预告内容",
    "perform_reason": "业绩变动原因",
    "net_profit_upper": "净利润变动幅度上限(%)",
    "net_profit_lower": "净利润变动幅度下限(%)",
    "update_date": "发布日期",
}, inplace=True)

stock_performance.to_csv('业绩报告.csv', index=False)
stock_forcast.to_csv('业绩预告.csv', index=False)