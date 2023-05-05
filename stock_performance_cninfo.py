import os
os.chdir('/home/project/stocker')

import pandas as pd
import pymysql
import sys
import stock_config as sc
import requests as rq 
from py_mini_racer import py_mini_racer
import time
import json

db_config = sc.DB_CONFIG
REPORT_DATE = sys.argv[1]

# market 012001=沪市 012002=深市
def stock_performance(url = 'http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1041', params={'rdate': '20230331', 'market': '012001'}):
    random_time_str = str(int(time.time()))
    js_code = py_mini_racer.MiniRacer()
    js_str = """
        function mcode(input) {  
                    var keyStr = "ABCDEFGHIJKLMNOP" + "QRSTUVWXYZabcdef" + "ghijklmnopqrstuv"   + "wxyz0123456789+/" + "=";  
                    var output = "";  
                    var chr1, chr2, chr3 = "";  
                    var enc1, enc2, enc3, enc4 = "";  
                    var i = 0;  
                    do {  
                        chr1 = input.charCodeAt(i++);  
                        chr2 = input.charCodeAt(i++);  
                        chr3 = input.charCodeAt(i++);  
                        enc1 = chr1 >> 2;  
                        enc2 = ((chr1 & 3) << 4) | (chr2 >> 4);  
                        enc3 = ((chr2 & 15) << 2) | (chr3 >> 6);  
                        enc4 = chr3 & 63;  
                        if (isNaN(chr2)) {  
                            enc3 = enc4 = 64;  
                        } else if (isNaN(chr3)) {  
                            enc4 = 64;  
                        }  
                        output = output + keyStr.charAt(enc1) + keyStr.charAt(enc2)  
                                + keyStr.charAt(enc3) + keyStr.charAt(enc4);  
                        chr1 = chr2 = chr3 = "";  
                        enc1 = enc2 = enc3 = enc4 = "";  
                    } while (i < input.length);  
                    return output;  
                }  
    """
    js_code.eval(js_str)
    mcode = js_code.call('mcode', random_time_str)
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Cache-Control': 'no-cache',
        'Content-Length': '0',
        'Host': 'webapi.cninfo.com.cn',
        'mcode': mcode,
        'Origin': 'http://webapi.cninfo.com.cn',
        'Pragma': 'no-cache',
        'Proxy-Connection': 'keep-alive',
        'Referer': 'http://webapi.cninfo.com.cn/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
    }
    res = rq.post(url=url, headers=headers, params=params)
    res_json = json.loads(res.text)
    return {'text': res.text, 'json': res_json}

def upsert_performance(df):
    rename_cols = {
        'F001D': 'report_date',
        'SECCODE': 'code',
        'SECNAME': 'name',
        'F009N': 'holder_net_income',
        'F006N': 'operate_income',
        'F008N': 'operate_profit',
        'F007N': 'operate_cost',
        'F005N': 'operate_share_flow',
        'F003N': 'share_net_asset',
        'F002N': 'share_earn',
        'F004N': 'roe',
        'F011N': 'operate_margin',
        'F010N': 'gross_profit_margin',
    }
    df_upsert = df.rename(columns=rename_cols)
    df_upsert = df_upsert.fillna(-1)
    df_upsert = df_upsert[['report_date', 'code', 'name', 'holder_net_income', 'operate_income', 'operate_profit', 'operate_cost', 'operate_share_flow', 'share_net_asset', 'share_earn', 'roe', 'operate_margin', 'gross_profit_margin']]
    cnx = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'], database=db_config['database'])
    cursor = cnx.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_performance (
            report_date date not null comment '报告期',
            code varchar(30) not null comment '证券代码',
            name varchar(30) comment '证券简称',
            holder_net_income float comment '归属母公司所有者净利润(元)',
            operate_income float comment '营业收入(元)',
            operate_profit float comment '营业利润(元)',
            operate_cost float comment '营业成本(元)',        
            operate_share_flow float comment '每股经营现金流量(元)',
            share_net_asset float comment '每股净资产(元)',
            share_earn float comment '每股收益(元)',
            roe float comment '净资产收益率',
            operate_margin float comment '营业利润率%',
            gross_profit_margin float comment '毛利率%',
            PRIMARY KEY (report_date, code)
        )
    """)
    # Upsert the data frame into the stock_performance table
    cursor.executemany("""
        INSERT INTO stock_performance (
            report_date, 
            code, 
            name, 
            holder_net_income, 
            operate_income, 
            operate_profit, 
            operate_cost, 
            operate_share_flow, 
            share_net_asset, 
            share_earn, 
            roe, 
            operate_margin, 
            gross_profit_margin
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        name = VALUES(name),
        holder_net_income = VALUES(holder_net_income),
        operate_income = VALUES(operate_income),
        operate_profit = VALUES(operate_profit),
        operate_cost = VALUES(operate_cost),
        operate_share_flow = VALUES(operate_share_flow),
        share_net_asset = VALUES(share_net_asset),
        share_earn = VALUES(share_earn),
        roe = VALUES(roe),
        operate_margin = VALUES(operate_margin),
        gross_profit_margin = VALUES(gross_profit_margin)
    """, df_upsert.values.tolist())
    cnx.commit()
    return "Succeed"

def stock_forcast(url='http://www.cninfo.com.cn/data20/performanceForecast/list?rdate=20230331'):
    headers = {
        'Content-Type': 'application/json; charset=utf-8', 
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'en-US,en;q=0.9,zh-TW;q=0.8,zh;q=0.7',
        'Connection': 'keep-alive',
        'Host': 'webapi.cninfo.com.cn',
        'Origin': 'http://webapi.cninfo.com.cn',
        'Referer': 'http://webapi.cninfo.com.cn/',
        'DNT': 1,
        'X-Requested-With': 'XMLHttpRequest',
        'Content-Length': 0,
        'Cookie': 'Hm_lvt_489bd07e99fbfc5f12cbb4145adb0a9b={time_stamp}; Hm_lpvt_489bd07e99fbfc5f12cbb4145adb0a9b={time_stamp}'.format(time_stamp = int(time.time())),
        'mcode': 'MTY4MzEzMTYyMw==',
    }
    res = rq.post(url, headers)
    res_json = json.loads(res.text)
    return {'json':res_json, 'text':res}

def upsert_forcast(df):
    rename_cols = {
        'F001D': 'report_date',
        'SECCODE': 'code',
        'SECNAME': 'name',
        'DECLAREDATE': 'update_date',
        'F002V': 'industry_2nd',
        'F003V': 'perform_type',
        'F004V': 'forcast_content',
        'F005V': 'perform_reason',
        'F010N': 'net_profit_upper',
        'F009N': 'net_profit_lower',
    }
    df_upsert = df.rename(columns=rename_cols)
    df_upsert = df_upsert.fillna(-1)
    df_upsert = df_upsert[['report_date', 'code', 'name', 'industry_2nd', 'perform_type', 'forcast_content', 'perform_reason', 'net_profit_upper', 'net_profit_lower', 'update_date']]
    cnx = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'], database=db_config['database'])
    cursor = cnx.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_forcast (
            report_date date not null comment '报告年度',
            code varchar(30) not null comment '证券代码',
            name varchar(30) comment '证券简称',
            industry_2nd varchar(30) comment '二级行业',
            perform_type varchar(30) comment '业绩变动类型',
            forcast_content varchar(1000) comment '业绩预告内容',
            perform_reason varchar(1000) comment '业绩变动原因',
            net_profit_upper float comment '净利润变动幅度上限(%)',
            net_profit_lower float comment '净利润变动幅度下限(%)',
            update_date date comment '发布日期',
            PRIMARY KEY (report_date, code)
        )
    """)
    cursor.executemany("""
        INSERT INTO stock_forcast (
            report_date, 
            code, 
            name, 
            industry_2nd, 
            perform_type, 
            forcast_content, 
            perform_reason, 
            net_profit_upper, 
            net_profit_lower, 
            update_date
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            industry_2nd = VALUES(industry_2nd),
            perform_type = VALUES(perform_type),
            forcast_content = VALUES(forcast_content),
            perform_reason = VALUES(perform_reason),
            net_profit_upper = VALUES(net_profit_upper),
            net_profit_lower = VALUES(net_profit_lower),
            update_date = VALUES(update_date)
    """, df_upsert.values.tolist())
    cnx.commit()
    return "Succeed"

# 业绩报告
print('报告日期：{}'.format(REPORT_DATE))
print('业绩报告下载中...')
stock_perform_sh = stock_performance(params={'rdate': REPORT_DATE.replace('-',''), 'market': '012001'})
df_perform_sh = pd.DataFrame(stock_perform_sh['json']['records'])
upsert_performance(df_perform_sh)
print('SH - {}'.format(df_perform_sh.shape))
stock_perform_sz = stock_performance(params={'rdate': REPORT_DATE.replace('-',''), 'market': '012002'})
df_perform_sz = pd.DataFrame(stock_perform_sz['json']['records'])
upsert_performance(df_perform_sz)
print('SZ - {}'.format(df_perform_sz.shape))
print('业绩报告下载成功...')

# 业绩预告
print('业绩预告下载中...')
stock_forcast = stock_forcast(url='http://www.cninfo.com.cn/data20/performanceForecast/list?rdate={}'.format(REPORT_DATE.replace('-','')))
df_forcast = pd.DataFrame(stock_forcast['json']['data']['records'])
upsert_forcast(df_forcast)
print('业绩预告下载成功...')