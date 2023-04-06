#!/bin/bash
echo "----- start - minute -----" >> /home/project/stocker/log.txt
echo `date` "| stock_prices_1min.py" >> /home/project/stocker/log.txt | /home/project/stocker/stocker_env/bin/python /home/project/stocker/stock_prices_1min.py 
echo "----- end - minute -----" >> /home/project/stocker/log.txt