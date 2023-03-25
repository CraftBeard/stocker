import pymysql
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

# Connect to MySQL database
mydb = pymysql.connect(
  host="localhost",
  user="stocker",
  password="2016@uq$tencent",
  database="stock"
)

# Get data from MySQL table
stocks_data = pd.read_sql("SELECT date, code, high, low, close FROM stock_prices WHERE date >= DATE_SUB(NOW(), INTERVAL 24 MONTH)", con=mydb)
# Get code and code_name from MySQL table
codes = pd.read_sql("SELECT code, code_name FROM stock_codes", con=mydb)
# Merge with data to get code_name for each code
stocks_data = pd.merge(stocks_data, codes, on='code', how='left')

unique_codes = stocks_data['code'].unique()

# Set Chinese font
font = FontProperties(fname='/System/Library/Fonts/PingFang.ttc')

fig, axs = plt.subplots(len(unique_codes), 1, figsize=(10, 30)) # changed figsize to leave more distance between subplots

for i, code in enumerate(unique_codes):
    code_data = stocks_data[stocks_data['code'] == code]
    code_name = code_data['code_name']
    date_values = code_data['date']
    close_values = code_data['close']
    high_values = code_data['high']
    low_values = code_data['low']

    # Draw subplot chart
    axs[i].plot(date_values, high_values, linestyle='--', color='red')
    axs[i].plot(date_values, low_values, linestyle='--', color='green')
    axs[i].plot(date_values, close_values, color='blue')
    axs[i].set_title(f"{code} - {code_name}", fontproperties=font) # Set Chinese font

    # Add tags to lowest close values
    lowest_close_3m = min(close_values[date_values >= pd.Timestamp('now') - pd.Timedelta(days=90)])
    axs[i].text(date_values.iloc[-1], lowest_close_3m, f"3-month lowest close: {lowest_close_3m}", fontsize=8, color='blue', fontproperties=font) # Set Chinese font

    lowest_close_6m = min(close_values[date_values >= pd.Timestamp('now') - pd.Timedelta(days=180)])
    axs[i].text(date_values.iloc[-1], lowest_close_6m, f"6-month lowest close: {lowest_close_6m}", fontsize=8, color='blue', fontproperties=font) # Set Chinese font

    lowest_close_12m = min(close_values[date_values >= pd.Timestamp('now') - pd.Timedelta(days=365)])
    axs[i].text(date_values.iloc[-1], lowest_close_12m, f"12-month lowest close: {lowest_close_12m}", fontsize=8, color='blue', fontproperties=font) # Set Chinese font

    lowest_close_24m = min(close_values)
    axs[i].text(date_values.iloc[-1], lowest_close_24m, f"24-month lowest close: {lowest_close_24m}", fontsize=8, color='blue', fontproperties=font) # Set Chinese font

plt.savefig(f"stocks.png")
plt.clf()

stocks_data.to_csv('stocks_data.csv', index=False)
