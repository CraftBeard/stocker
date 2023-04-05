import os
os.chdir('/home/project/stocker')

import stock_config as sc
stocks = [stock.replace('.', '') for stock in sc.STOCK_CONFIG['stocks']]
db_config = sc.DB_CONFIG

import Ashare
import pymysql
import datetime
import pandas as pd


def stock_prices_realtime(stocks, freq='1m', cnt=7):
    """
    Get the real-time stock prices for a list of stocks.
    :param stocks: list of stock codes
    :param freq: frequency of the stock prices
    :param cnt: number of stock prices to retrieve
    :return: pandas dataframe of stock prices
    """
    stock_datas = []
    for stock in stocks:
        stock_data = Ashare.get_price(stock,frequency=freq,count=cnt)
        stock_data['code'] = stock
        stock_datas.append(stock_data)
    out_df = pd.concat(stock_datas).reset_index()
    out_df = out_df.rename(columns={out_df.columns[0]:'dt'})
    return out_df


def insert_dataframe_to_mysql(dataframe, table_name, db_config):
    """
    Insert a pandas dataframe into a MySQL table.
    :param dataframe: pandas dataframe to insert
    :param table_name: name of the MySQL table
    :param db_config: database configuration dictionary
    """
    try:
        connection = pymysql.connect(host=db_config['host'],
                                             database=db_config['database'],
                                             user=db_config['user'],
                                             password=db_config['password'])
        cursor = connection.cursor()
        create_table = """
        CREATE TABLE IF NOT EXISTS {} (
            {} DATETIME, 
            {} VARCHAR(20),
            {} FLOAT, 
            {} FLOAT, 
            {} FLOAT, 
            {} FLOAT,
            {} FLOAT,
            PRIMARY KEY (dt, code)
        )
        """.format(table_name, 'dt', 'code', 'open', 'close', 'high', 'low', 'volume')
        cursor.execute(create_table)
        dataframe_columns = list(dataframe)
        columns = ",".join(dataframe_columns)
        values = "VALUES({})".format(",".join(["%s" for _ in dataframe_columns]))
        insert_sql = "INSERT INTO {} ({}) {}".format(table_name, columns, values)
        on_duplicate_key_update = " ON DUPLICATE KEY UPDATE " + ", ".join([f"{col} = VALUES({col})" for col in dataframe_columns[1:]])         
        insert_sql += on_duplicate_key_update
        records = dataframe.to_records(index=False)
        insert_records = [tuple(record) for record in records]
        cursor.executemany(insert_sql, insert_records)
        connection.commit()
        print(len(insert_records), "Record inserted successfully into {} table".format(table_name))    
    except pymysql.Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if (connection.open):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


now = datetime.datetime.now()
if now.weekday() < 5 and now.minute%5==1:
    stock_data = stock_prices_realtime(stocks)
    insert_dataframe_to_mysql(stock_data, 'stock_prices_1min', db_config)
