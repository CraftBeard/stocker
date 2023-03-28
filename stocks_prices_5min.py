import os
os.chdir('/home/project/stocker')

import stock_config as sc
stocks = [stock.replace('.', '') for stock in sc.STOCK_CONFIG['stocks']]
db_config = sc.DB_CONFIG

import Ashare
import pymysql
import datetime


# Define function to get stock prices in real-time
def stock_prices_realtime(stocks, freq='5m', cnt=2):
    stock_datas = []
    for stock in stocks:
        stock_data = Ashare.get_price(stock,frequency=freq,count=cnt)
        stock_data['stock'] = stock
        stock_datas.append(stock_data)
    return pd.concat(stock_datas)


# Define function to insert data into MySQL database
def insert_dataframe_to_mysql(dataframe, table_name, db_config):
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
            PRIMARY KEY (dt, code)
        )
        """.format('stock_prices_realtime', 'dt', 'code', 'open', 'high', 'low', 'volume')
        cursor.execute(create_table)
        dataframe_columns = list(dataframe)
        columns = ",".join(dataframe_columns)
        values = "VALUES({})".format(",".join(["%s" for _ in dataframe_columns]))
        insert_statement = "INSERT INTO {} ({}) {}".format(table_name, columns, values)
        on_duplicate_key_update = " ON DUPLICATE KEY UPDATE " + ", ".join([f"{col} = VALUES({col})" for col in dataframe_columns[2:]])
        insert_statement += on_duplicate_key_update
        records = dataframe.to_records(index=False)
        insert_records = [tuple(record) for record in records]
        cursor.executemany(insert_statement, insert_records)
        connection.commit()
        print(len(insert_records), "Record inserted successfully into {} table".format(table_name))
    except pymysql.Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if (connection.open):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


# Get current time and check if it is the first minute of a 5-minute interval
now = datetime.datetime.now()
if now.minute%5==1:
    # Get real-time stock prices
    stock_data = stock_prices_realtime(stocks)
    # Insert stock prices into MySQL database
    insert_dataframe_to_mysql(stock_data, 'stock_prices', db_config)
