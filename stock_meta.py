import baostock as bs
import mysql.connector


cnx = mysql.connector.connect(user='stocker', password='2016@uq$tencent',
                               host='localhost',
                               database='stocker')
cursor = cnx.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS stock_codes (
        code VARCHAR(50) PRIMARY KEY, 
        tradestatus VARCHAR(10), 
        code_name VARCHAR(100)
    )
""")
lg = bs.login()
rs = bs.query_all_stock()
while (rs.error_code == '0') & rs.next():
    stock_data = rs.get_row_data()
    cursor.execute("""
        INSERT INTO stock_codes (code, tradestatus, code_name) 
        VALUES (%s) 
        ON DUPLICATE KEY UPDATE code=code
    """, (stock_data))
cnx.commit()
cursor.close()
cnx.close()
bs.logout()
