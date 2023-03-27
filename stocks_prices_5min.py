import Ashare

for stock in stocks:
    stock_data = Ashare.get_price(stock,frequency='1d',count=10)
    print(stock_data)

