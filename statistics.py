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
    code_name = code_data['code_name'].iloc[0]
    date_values = code_data['date']
    close_values = code_data['close']
    high_values = code_data['high']
    low_values = code_data['low']

    axs[i].plot(date_values, high_values, linestyle='--', color='red')
    axs[i].plot(date_values, low_values, linestyle='--', color='green')
    axs[i].plot(date_values, close_values, color='blue')
    axs[i].set_title(f"{code} - {code_name}", fontproperties=font)

    # Get dates of lowest_close
    lowest_close_3m = min(close_values[date_values >= pd.Timestamp('now') - pd.Timedelta(days=90)])
    date_3m = max(date_values[close_values == lowest_close_3m])
    lowest_close_6m = min(close_values[date_values >= pd.Timestamp('now') - pd.Timedelta(days=180)])
    date_6m = max(date_values[close_values == lowest_close_6m])
    lowest_close_12m = min(close_values[date_values >= pd.Timestamp('now') - pd.Timedelta(days=365)])
    date_12m = max(date_values[close_values == lowest_close_12m])
    lowest_close_24m = min(close_values)
    date_24m = max(date_values[close_values == lowest_close_24m])
    axs[i].annotate(f"3-month lowest: {lowest_close_3m} ({date_3m})", xy=(date_3m, lowest_close_3m), xytext=(-10,-10), textcoords='offset points', arrowprops=dict(facecolor='blue', arrowstyle='->'), fontsize=8, color='orange', fontproperties=font)
    axs[i].annotate(f"6-month lowest: {lowest_close_6m} ({date_6m})", xy=(date_6m, lowest_close_6m), xytext=(10,10), textcoords='offset points', arrowprops=dict(facecolor='blue', arrowstyle='->'), fontsize=8, color='pink', fontproperties=font)
    axs[i].annotate(f"12-month lowest: {lowest_close_12m} ({date_12m})", xy=(date_12m, lowest_close_12m), xytext=(20,20), textcoords='offset points', arrowprops=dict(facecolor='blue', arrowstyle='->'), fontsize=8, color='canya', fontproperties=font)
    axs[i].annotate(f"24-month lowest: {lowest_close_24m} ({date_24m})", xy=(date_24m, lowest_close_24m), xytext=(-20,-20), textcoords='offset points', arrowprops=dict(facecolor='blue', arrowstyle='->'), fontsize=8, color='purple', fontproperties=font)
    fig.tight_layout()

# Save the plot
plt.savefig(f"stocks.png")

# Clear the plot
plt.clf()

# Save the stock prices data to a CSV file
stocks_data.to_csv('stocks_data.csv', index=False)
