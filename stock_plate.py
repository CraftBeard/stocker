
import requests
import pandas as pd
from bs4 import BeautifulSoup

# Set the headers for the crawler
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

# Send a GET request to the URL with the headers
url = 'https://data.eastmoney.com/bkzj/hy.html'
response = requests.get(url, headers=headers)

# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(response.content, 'html.parser')

# Find the table element using its class name
table = soup.find('table')

# Extract the table headers and rows
headers = [th.text.strip() for th in table.find_all('th')]
rows = []
for tr in table.find_all('tr'):
    row = [td.text.strip() for td in tr.find_all('td')]
    if row:
        rows.append(row)

# Create a pandas dataframe from the headers and rows
df = pd.DataFrame(rows, columns=headers)

# Print the dataframe
print(df)
