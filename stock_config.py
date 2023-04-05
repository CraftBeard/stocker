# stock_config.py
import os

DB_CONFIG = {
    'user': os.getenv('STOCKER_MYSQL_USER'),
    'password': os.getenv('STOCKER_MYSQL_PASS'),
    'host': os.getenv('STOCKER_MYSQL_HOST'),
    'database': os.getenv('STOCKER_MYSQL_DB'),
}

EMAIL_CONFIG = {
    'from' : os.getenv('EMAIL_USER'),
    'to' : ['lianglx.alex@gmail.com'],
    'subject' : '[Auto] Stock Data',
    'password' : os.getenv('EMAIL_PASS'),
    'smtp_server': os.getenv('EMAIL_SMTP'),
    'smtp_port': os.getenv('EMAIL_SMTP_PORT')
}

STOCK_CONFIG = {
    'stocks' : ['sh.600036', 'sh.601012', 'sz.003026', 'sh.603396', 'sz.002594', 'sh.600519', 'sz.300693', 'sz.300001','sz.000821']
}
