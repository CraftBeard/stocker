import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# Connect to MySQL database
mydb = pymysql.connect(
  host="localhost",
  user="stocker",
  password="2016@uq$tencent",
  database="stock"
)

# Get data from MySQL table
data = pd.read_sql("SELECT date, code, high, low, close FROM stock_prices WHERE date >= DATE_SUB(NOW(), INTERVAL 24 MONTH)", con=mydb)

unique_codes = data['code'].unique()

fig, axs = plt.subplots(len(unique_codes), 1, figsize=(10, 30)) # changed figsize to leave more distance between subplots

for i, code in enumerate(unique_codes):
    code_data = data[data['code'] == code]
    date_values = code_data['date']
    close_values = code_data['close']
    high_values = code_data['high']
    low_values = code_data['low']

    # Draw subplot chart
    axs[i].plot(date_values, high_values, linestyle='--', color='red')
    axs[i].plot(date_values, low_values, linestyle='--', color='green')
    axs[i].plot(date_values, close_values, color='blue')
    axs[i].set_title(code)

plt.savefig(f"stocks.png")
plt.clf()
