import baostock as bs
import pandas as pd
import pymysql
import sys

stocks = ["sh.600036", "sh.601012", "sz.002594", "sh.688598", "sh.300001", "sh.600519", "sh.600389", "sz.300316"]

# login to mysql
conn = pymysql.connect(host='localhost', user='stocker', password='2016@uq$tencent', database='stock')

# create mysql table if not exists
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS stock_prices (
                    date DATE,
                    code VARCHAR(10),
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume FLOAT,
                    amount FLOAT,
                    preclose FLOAT,
                    adjustflag INT,
                    turn FLOAT,
                    tradestatus INT,
                    pctChg FLOAT,
                    isST INT,
                    PRIMARY KEY (date, code)
                )''')

# get stock data from baostock
lg = bs.login()
for stock in stocks:
    rs = bs.query_history_k_data_plus(stock, "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST", start_date=sys.argv[1], end_date=sys.argv[2], frequency="d", adjustflag="3")
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    result = pd.DataFrame(data_list, columns=rs.fields)

    # insert data into mysql
    for index, row in result.iterrows():
        try:
            if row.isnull().values.any():
                continue
            sql = f"""
            INSERT IGNORE INTO stock_prices (
                date, 
                code, 
                open, 
                high, 
                low, 
                close, 
                volume, 
                amount, 
                preclose, 
                adjustflag, 
                turn, 
                tradestatus, 
                pctChg, 
                isST
            ) VALUES (
                '{str(row['date'])}', 
                '{str(row['code'])}', 
                {float(row['open'].strip()) if row['open'].strip() != '' else 0}, 
                {float(row['high'].strip()) if row['high'].strip() != '' else 0}, 
                {float(row['low'].strip()) if row['low'].strip() != '' else 0}, 
                {float(row['close'].strip()) if row['close'].strip() != '' else 0}, 
                {float(row['volume'].strip()) if row['volume'].strip() != '' else 0}, 
                {float(row['amount'].strip()) if row['amount'].strip() != '' else 0}, 
                {float(row['preclose'].strip()) if row['preclose'].strip() != '' else 0}, 
                {int(row['adjustflag'].strip()) if row['adjustflag'].strip() != '' else 0}, 
                {float(row['turn'].strip()) if row['turn'].strip() != '' else 0}, 
                {int(row['tradestatus'].strip()) if row['tradestatus'].strip() != '' else 0}, 
                {float(row['pctChg'].strip()) if row['pctChg'].strip() != '' else 0}, 
                {int(row['isST'].strip()) if row['isST'].strip() != '' else 0}
            )
            """
            cursor.execute(sql)
        except Exception as e:
            print(f"Error occurred while inserting data into mysql: {e}")
            print(row)
conn.commit()

# logout from baostock and mysql
bs.logout()
cursor.close()
conn.close()
