import os
os.chdir('/home/project/stocker')

import pandas as pd
import pymysql
import sys
import stock_config as sc
import requests as rq 
from py_mini_racer import py_mini_racer
import time

db_config = sc.DB_CONFIG
QPS = 30

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

url = "http://webapi.cninfo.com.cn/api/sysapi/p_sysapi1041"
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
mcode = js_code.call("mcode", random_time_str)
headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Content-Length": "0",
    "Host": "webapi.cninfo.com.cn",
    "mcode": mcode,
    "Origin": "http://webapi.cninfo.com.cn",
    "Pragma": "no-cache",
    "Proxy-Connection": "keep-alive",
    "Referer": "http://webapi.cninfo.com.cn/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
}
params = {
    "rdate": "20230331",
    "market": "012002"
}
r = rq.post(url, headers=headers, params=params)
