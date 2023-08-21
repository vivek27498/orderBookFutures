from datetime import datetime
import threading
# import sys
import time
import signal
from pathlib import Path
from  database_connector import DatabaseConnector
from logger import setup_logger

from binance.client import Client
user_key = '57730420ff758f4b3efa3f0d5b86a2083103816eb5982808e32f0c1f2663bf5f'
secret_key = '5270cea3b14bd90260a46ed726e061fae1dfcc677d75bbfa1a79f4d8ab6aefea'
binance_client = Client(user_key, secret_key,base_endpoint='https://testnet.binancefuture.com/fapi/v1/order',tld='com',testnet=True)


SAMPLE_INTERVAL = 10 #(seconds)
ORDERBOOK_DEPTH = 20 # (lines)
EXCHANGE_MARKETS = ['BTCBUSD', 'ETHBUSD'] #ADA_USD #BTC_USD #ETH_USD

SYNC_TIME = 5 # (seconds) time before getting the first target
LOGGER_NAME = 'data_logger'

DATA_FILE_PATH = './/data'

SAVE_ORDER_IMBALANCE = True 
SAVE_ORDER_IMBALANCE_JUMP = True ## This variable controls when to save the order imbalance: False for saving it each SAMPLE_INTERVAL, True for every other SAMPLE_INTERVAL.

class DataLogger():

    def __init__(self):
        """
        All constants are defined on config.json.
        """
        self.data_folder_path = DATA_FILE_PATH
        Path(self.data_folder_path).mkdir(parents=True, exist_ok=True)

        self.logger = setup_logger(name=LOGGER_NAME, folder_path=self.data_folder_path, file_name='binance_orderbook.logs')
        self.logger.info('Starting logger...')

        self.running = False

        self.target = 0
        self.next_target = 0

        self.sample_interval = SAMPLE_INTERVAL
        self.orderbook_depth = ORDERBOOK_DEPTH
        self.exchange_markets = EXCHANGE_MARKETS

        self.client = Client(user_key, secret_key,base_endpoint='https://testnet.binancefuture.com/fapi/v1/order',tld='com',testnet=True)

        db_credentials = {
            "host":"localhost",
            "port":"5432",
            "database":"binance",
            "user": "postgres",
            "password":"vivek123"
        }
        self.db_connector = DatabaseConnector(db_credentials)

        self.orderbook_data = []
        self.order_imbalance_data = []

        self.save_order_imbalance_count = True # To control SAVE_ORDER_IMBALANCE_JUMP
        
    def run(self):

        self.running = True        

        self.validate_markets()

        #create order book tables 
        if not self.db_connector.create_orderbook_tables(self.exchange_markets) and self.running:
            self.running = False
            self.logger.error('Unable to create all orderbook tables. Ending program...')
        else:
            self.logger.info('All orderbook tables ready.')

        if SAVE_ORDER_IMBALANCE:
            if not self.db_connector.create_order_imbalance_tables(self.exchange_markets) and self.running:
                self.running = False
                self.logger.error('Unable to create all order imbalance tables. Ending program...')
            else:
                self.logger.info('All order imbalance tables ready.')

        if self.running:
            self.get_next_target()
            self.target = self.next_target
            self.logger.info('Syncing to first target at %s', datetime.utcfromtimestamp(self.target))

        
        while self.running:

            if time.time() >= self.target:

                self.logger.info('Target reached.')

                self.get_next_target()

                self.orderbook_data = []
                self.order_imbalance_data = []
            
                time_get_data = time.time()

                thread_list = []
                for market in self.exchange_markets:
                    thread_list.append(
                        threading.Thread(
                            target=self.get_data,
                            args=(market,),
                            name=f'get_data({market})',
                            )
                        )

                for thread in thread_list:
                    thread.start()

                for thread in thread_list:
                    thread.join()
                
                time_get_data = round(time.time() - time_get_data, 2)
                self.logger.info(f'get_data {time_get_data}s.')

                time_save_data = time.time()
                if not self.db_connector.insert_orderbook_data(self.orderbook_data, self.logger):
                    self.logger.error('Unable to save orderbook data.')

                if self.save_order_imbalance_count and SAVE_ORDER_IMBALANCE:
                    if not self.db_connector.insert_order_imbalance_data(self.order_imbalance_data, self.logger):
                        self.logger.error('Unable to save order imbalance data.')

                time_save_data = round(time.time() - time_save_data, 2)
                self.logger.info(f'save_data {time_save_data}s.')

                self.orderbook_data = []
                self.order_imbalance_data = []

                if SAVE_ORDER_IMBALANCE_JUMP:
                    self.save_order_imbalance_count = not self.save_order_imbalance_count

                self.target = self.next_target
                self.logger.info('Next target: %s', datetime.utcfromtimestamp(self.target))

            time.sleep(0.1)
            
        self.logger.info('Program ended.')

    def get_data(self, market):
        try:
            raw_orderbook_data = self.client.futures_order_book(symbol=market)

            if SAVE_ORDER_IMBALANCE and self.save_order_imbalance_count:

                order_imbalance = {
                    'date': datetime.utcfromtimestamp(self.target).strftime("%Y-%m-%d %H:%M:%S%z+00"), 
                    'market': market,
                    'order_imbalance_bid': 0,
                    'order_imbalance_ask': 0
                }

                # 'bids': [['28299.29', '1.293'], ['28242.69', '5.226']] prize and size
                for bids in raw_orderbook_data['bids']:
                    order_imbalance['order_imbalance_bid'] = order_imbalance['order_imbalance_bid'] + (float(bids[1])*float(bids[0]))

                for asks in raw_orderbook_data['asks']:
                    order_imbalance['order_imbalance_ask'] = order_imbalance['order_imbalance_ask'] + (float(asks[1])*float(asks[0]))

                self.order_imbalance_data.append(order_imbalance)

            orderbook_data = [market, datetime.utcfromtimestamp(self.target).strftime("%Y-%m-%d %H:%M:%S%z+00")]
            for i in range(20):
                if i < len(raw_orderbook_data['bids']):
                    orderbook_data.append(raw_orderbook_data['bids'][i][0])
                    orderbook_data.append(raw_orderbook_data['bids'][i][1])
                else:
                    orderbook_data.extend([0,0])
                if i < len(raw_orderbook_data['asks']):
                    orderbook_data.append(raw_orderbook_data['asks'][i][0])
                    orderbook_data.append(raw_orderbook_data['asks'][i][1])
                else:
                    orderbook_data.extend([0,0])

            self.orderbook_data.append(orderbook_data)
            
        except Exception as e:
            self.logger.error(e)
            self.logger.error('Unable to get data for %s.', market)
   
    def validate_markets(self):
        """
        Validates that the markets in EXCHANGE_MARKETS are available in the exchange.

        Exits if any market is no valid.
        """
        try:
            exchange_market_list = self.client.get_exchange_info()
            market_symbols = [market['symbol'] for market in exchange_market_list['symbols']]
            # print(market_symbols)


        except Exception as e:
            self.logger.error(e)
            self.logger.error('Unable to get markets. Ending program...')
            self.running = False

        for market in self.exchange_markets:
            if market not in market_symbols:
                self.logger.error('Invalid market: %s', market)
                self.running = False
                return
        
        self.logger.info('All markets are valid.')    

    def get_next_target(self):
        """
        updates self.next_target with the new target
        """
        sample_interval = self.sample_interval # 10 seconds
        time_now = time.time()
        delta = sample_interval - (time_now%sample_interval)
        self.next_target = time_now + delta


    def stop(self):
        self.running = False
        self.logger.info('Ending program. Waiting for cycle to finish...')

    
def main():
    """
    Main function.
    """
    data_logger = DataLogger()

    def signal_handler(signum, frame):
        data_logger.stop()

    signal.signal(signal.SIGINT, signal_handler)

    data_logger.run()

if __name__ == '__main__':
    main()