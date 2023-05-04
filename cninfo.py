# curl -H 'Content-Type: application/json' -X POST http://www.cninfo.com.cn/data20/performanceForecast/list?rdate=20230331

import requests as rq
import json
import pandas as pd
import time


def stock_spider(url='', data={}):
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
    print(url)
    print(headers)
    res = rq.post(url, headers, json=data)
    print(res)
    res_json = json.loads(res.text)
    return {'json':res_json, 'text':res}

# 业绩预告
forcast_url = 'http://www.cninfo.com.cn/data20/performanceForecast/list?rdate=20230331'
# forcast_url = 'http://www.cninfo.com.cn/data20/performanceForecast/list?rdate=20221231'
#url = 'http://www.cninfo.com.cn/data20/performanceForecast/list?rdate=20220930'
forcast_res = stock_spider(url=forcast_url)
forcast_df = pd.DataFrame(forcast_res['json']['data']['records'])
# forcast_df.shape
# forcast_df.head()

# 业绩报告
