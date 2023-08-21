"""
Example on how to use database_connector module.
"""
import time
import json
from  database_connector import DatabaseConnector
import sys
import config
from datetime import datetime

# Example parameters.
start_time = '2023-07-08 01:00:00+00'
end_time = '2023-07-09 09:00:00+00'


program_start_time = time.time()

print(f'Program start time {datetime.fromtimestamp(program_start_time).isoformat()}')

# Getting credentials from file.
filepath = './dbCredentials.json'

with open(filepath, 'r', encoding='utf-8') as read_file:
    db_credentials = json.load(read_file)


# Initialize the client.
db_connector = DatabaseConnector(db_credentials)

print('Get list of markets from database:')

markets = ['BTCBUSD', 'ETHBUSD']

for market in markets:
    
    print(f'Get Candles Data from the database for market  {market}:')
    data = db_connector.get_orderbook(market, start_time, end_time)
    # print(data,"\n")
    print(f'{len(data)} rows of candles data downloaded for  {market}')

program_end_time = time.time()
print(f'Program end time {datetime.fromtimestamp(program_end_time).isoformat()}')
print(f'Total Time Taken - {(program_end_time - program_start_time)} seconds')