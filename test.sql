select
    date, code, count(1) as cnt, 
    max(high) as max_high, min(high) as min_high,
    max(low) as max_low, min(low) as min_low, 
    max(close) as max_close, min(close) as min_close
from stock_prices
group by date, code
order by code, date;

select min(date), count(1), count(*) from stock.stock_prices;