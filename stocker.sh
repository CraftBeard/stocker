#!/bin/bash
echo "----- start -----" >> /home/project/stocker/log.txt
echo `date` "|" stock_meta.py >> /home/project/stocker/log.txt | /home/project/stocker/stocker_env/bin/python /home/project/stocker/stock_meta.py
echo `date` "|" stock_prices.py `date +%Y-%m-%d` `date +%Y-%m-%d` >> /home/project/stocker/log.txt | /home/project/stocker/stocker_env/bin/python /home/project/stocker/stock_prices.py `date +%Y-%m-%d` `date +%Y-%m-%d`
echo `date` "|" stock_stats.py >> /home/project/stocker/log.txt | /home/project/stocker/stocker_env/bin/python /home/project/stocker/stock_stats.py
echo `date` "|" stock_email.py >> /home/project/stocker/log.txt | /home/project/stocker/stocker_env/bin/python /home/project/stocker/stock_email.py
echo "----- end -----" >> /home/project/stocker/log.txt