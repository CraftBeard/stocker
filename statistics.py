import pymysql
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

# Connect to the database
mydb = pymysql.connect(
  host="localhost",
  user="stocker",
  password="2016@uq$tencent",
  database="stock"
)

# Read stock prices data from the database
stocks_data = pd.read_sql("SELECT DISTINCT date, code, high, low, close FROM stock_prices WHERE date >= DATE_SUB(NOW(), INTERVAL 24 MONTH)", con=mydb)

# Read stock codes data from the database
codes = pd.read_sql("SELECT DISTINCT code, code_name FROM stock_codes", con=mydb)

# Merge stock prices and stock codes data
stocks_data = pd.merge(stocks_data, codes, on='code', how='left')

# Get unique codes
unique_codes = stocks_data['code'].unique()

# Set font properties
font = FontProperties(fname='fonts/MSYH.TTC')

# Create subplots for each unique code
fig, axs = plt.subplots(len(unique_codes), 1, figsize=(10, 30))

# Plot stock prices for each unique code
for i, code in enumerate(unique_codes):
    code_data = stocks_data[stocks_data['code'] == code]
    code_name = code_data['code_name']
    date_values = code_data['date']
    close_values = code_data['close']
    high_values = code_data['high']
    low_values = code_data['low']

    axs[i].plot(date_values, high_values, linestyle='--', color='red')
    axs[i].plot(date_values, low_values, linestyle='--', color='green')
    axs[i].plot(date_values, close_values, color='blue')
    axs[i].set_title(f"{code} - {code_name}", fontproperties=font)

    # Annotate 3-month lowest close
    lowest_close_3m = min(close_values[date_values >= pd.Timestamp('now') - pd.Timedelta(days=90)])
    axs[i].annotate(f"3-month lowest close: {lowest_close_3m}", xy=(date_values.iloc[-1], lowest_close_3m), xytext=(date_values.iloc[-1]+pd.Timedelta(days=30), lowest_close_3m+5), arrowprops=dict(facecolor='blue', arrowstyle='->'), fontsize=8, color='blue', fontproperties=font)

    # Annotate 6-month lowest close
    lowest_close_6m = min(close_values[date_values >= pd.Timestamp('now') - pd.Timedelta(days=180)])
    axs[i].annotate(f"6-month lowest close: {lowest_close_6m}", xy=(date_values.iloc[-1], lowest_close_6m), xytext=(date_values.iloc[-1]+pd.Timedelta(days=30), lowest_close_6m+5), arrowprops=dict(facecolor='blue', arrowstyle='->'), fontsize=8, color='blue', fontproperties=font)

    # Annotate 12-month lowest close
    lowest_close_12m = min(close_values[date_values >= pd.Timestamp('now') - pd.Timedelta(days=365)])
    axs[i].annotate(f"12-month lowest close: {lowest_close_12m}", xy=(date_values.iloc[-1], lowest_close_12m), xytext=(date_values.iloc[-1]+pd.Timedelta(days=30), lowest_close_12m+5), arrowprops=dict(facecolor='blue', arrowstyle='->'), fontsize=8, color='blue', fontproperties=font)

    # Annotate 24-month lowest close
    lowest_close_24m = min(close_values)
    axs[i].annotate(f"24-month lowest close: {lowest_close_24m}", xy=(date_values.iloc[-1], lowest_close_24m), xytext=(date_values.iloc[-1]+pd.Timedelta(days=30), lowest_close_24m+5), arrowprops=dict(facecolor='blue', arrowstyle='->'), fontsize=8, color='blue', fontproperties=font)

# Save the plot
plt.savefig(f"stocks.png")

# Clear the plot
plt.clf()

# Save the stock prices data to a CSV file
stocks_data.to_csv('stocks_data.csv', index=False)
