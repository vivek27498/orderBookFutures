"""
database connector V2
Module to connect to the server database and download data.
"""

import psycopg2
import psycopg2.extras
import pytz
from datetime import datetime

class DatabaseConnector:
    """
    Class to interact with the database.

    db_credentials must be provided in the form of a dict. Example:
        db_credentials = {
            "host": "localhost",
            "port": "5432",
            "database": "example_data",
            "user": "example_user",
            "password": "1234"
        }
    """

    def __init__(self, db_credentials):
        self.host = db_credentials['host']
        self.port = db_credentials['port']
        self.database = db_credentials['database']
        self.user = db_credentials['user']
        self.password = db_credentials['password']

        self.conn = None
        self.cur = None

        self.connected = False


    def connect(self):
        """
        Connect to the server.
        Sets self.connected True if successful, False if not.
        """
        try:
            if not self.connected:
                self.conn = psycopg2.connect(
                    host = self.host,
                    port = self.port,
                    database = self.database,
                    user = self.user,
                    password = self.password
                )
                self.cur = self.conn.cursor()

                self.connected = True

        except psycopg2.DatabaseError as error:
            print(error)
            self.connected = False
        
        return self.connected


    def disconnect(self):
        """
        Disconnects from the server.
        """
        if self.connected:
            self.cur.close()
            self.conn.close()
            self.connected = False
        

    def send_request(self, msg):
        """
        Send request to the database and returns response.
            Args:
                msg -> SQL string.
            Returns:
                response in list format.
                Empty list if couldn't execute.
        """
        disc_at_exit = False
        if not self.connected:
            self.connect()
            disc_at_exit = True

        if self.connected:
            try:
                self.cur.execute(msg)
                self.conn.commit()
                raw_response = self.cur.fetchall()
                
                if disc_at_exit:
                    self.disconnect()

                return raw_response

            except (psycopg2.DatabaseError) as error:
                print(error)
                return []

        return []

    
    def authentication_test(self, db_credentials):
        """
        Test server connection to authenticate user credentials.
        Returns True for a successful connection, False if not.
        """
        try:
            conn = psycopg2.connect(
                host = db_credentials['host'],
                port = db_credentials['port'],
                database = db_credentials['database'],
                user = db_credentials['user'],
                password = db_credentials['password']
            )

            cur = conn.cursor()

            cur.close()
            conn.close()

            return True

        except psycopg2.Error as error:
            print(error)
            return False

    
    def update_credentials(self, db_credentials):
        """
        Update client credentials.
        """
        self.host = db_credentials['host']
        self.port = db_credentials['port']
        self.database = db_credentials['database']
        self.user = db_credentials['user']
        self.password = db_credentials['password']

    ## ORDER BOOKS

    def create_orderbook_tables(self, markets_list):
        """
        Admin credentials required.
        """
        if not isinstance(markets_list, list):
            markets_list = [markets_list]

        self.connect()

        for market in markets_list:

            table_name = f'orderbook_{market}'.lower().replace('-','_')

            header_base = ['bid_price', 'bid_volume', 'ask_price', 'ask_volume']
            header = ', '.join([f"{col}_{i} REAL" for i in range(1, 21) for col in header_base])

            msg = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                        date TIMESTAMP WITH TIME ZONE UNIQUE NOT NULL,
                        {header}
                    );

                    CREATE UNIQUE INDEX IF NOT EXISTS idx_ob_{market.lower().replace('-','_')} 
                    ON {table_name} (date);
                    GRANT SELECT ON TABLE {table_name} TO "read_only";
                    GRANT ALL ON TABLE {table_name} TO "developer";
                    GRANT ALL ON TABLE {table_name} TO "logger";
                    """

            try:
                self.cur.execute(msg)
                self.conn.commit()
            except (psycopg2.DatabaseError) as error:
                print('Unable to create table for market ', market)
                print(error)
                return False
            
        self.disconnect()
        return True

    def insert_orderbook_data(self, orderbook_data, logger=None):
        """
        Stored data list od orderbook onto market orderbook table.
        Args:
            market (str)
            orderbook_data (list): [[timestamp1, bid_price1 ... ask_volume20], [timestamp2, bid_price1 ... ask_volume20]...]
        """

        self.connect()

        if self.connected:
            for data in orderbook_data:
                market = data.pop(0)
                
                table_name = f'orderbook_{market}'.lower().replace('-','_')  

                args_str = ', '.join(['%s'] * 81)

                header_base = ['bid_price', 'bid_volume', 'ask_price', 'ask_volume']
                header = ', '.join([f"{col}_{i}" for i in range(1, 21) for col in header_base])

                header_conflict = ['EXCLUDED.bid_price', 'EXCLUDED.bid_volume', 'EXCLUDED.ask_price', 'EXCLUDED.ask_volume']
                header_conflict = ', '.join([f"{col}_{i}" for i in range(1, 21) for col in header_conflict])

                msg = f"""
                INSERT INTO {table_name} (
                    date, 
                    {header}
                ) VALUES ({args_str}) 
                ON CONFLICT (date) DO UPDATE 
                SET ({header}) = ({header_conflict});
                """

                try:
                    self.cur.execute(msg, data)
                    self.conn.commit()

                except Exception as e:
                    if logger:
                        logger.error(e)
                        logger.error(f'Unable to save {market} data.')
                    else:
                        print(e)
                        print(f'Unable to save {market} data.')
                    return False

            self.disconnect()
            return True

        return False
    
    def get_orderbook_markets(self):
        """
        Returns a list of the orderbooks markets available.
        Empty list in case of any error.
        """
        market_list = []
        self.connect()

        if self.connected:
            msg = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
                """

            try:
                self.cur.execute(msg)
                data = self.cur.fetchall()

                self.disconnect()

            except (psycopg2.DatabaseError) as error:
                print(error)
                return market_list
        
        filtered_list = [s for s in data if 'orderbook' in str(s)]
        
        for tables in filtered_list:
            market_list.append(tables[0].replace('orderbook_', '').replace('_', '-').upper())

        market_list.sort()

        return market_list

    def get_orderbook_market_info(self, markets_list):
        """
        Returns the date of the first record of orderbook for specific market or market list.
        """
        if not isinstance(markets_list, list):
            markets_list = [markets_list]

        market_info = {}

        self.connect()

        if self.connected:
            for market in markets_list:
                table_name = (f'orderbook_{market}'.lower()).replace('-','_')

                msg = f"""
                    SELECT date 
                    FROM {table_name}
                    ORDER BY date ASC
                    LIMIT 1;
                    """
                try:
                    self.cur.execute(msg)
                    data = self.cur.fetchall()

                except (psycopg2.DatabaseError) as error:
                    print(error)
                    return market_info

                if len(data) == 1:
                    market_info[market] =  data[0][0]
                else:
                    print(f'No data found on {market} orderbook.')
                    market_info[market] =  ''

            self.disconnect()

        return market_info

    def get_orderbook(self, market, start, end, period = 30):
        """
        Returns the orderbook data.

        Args: 
            market (str): desired market data.
            start, end (str): start and end date in format 'YYYY-MM-DD HH:MM:SS+00'
            period (int): number of seconds between samples.

        """
        # if not isinstance(markets_list, list):
        #     markets_list = [markets_list]

        period = int(period)

        data = {}

        self.connect()

        if self.connected:
            table_name = (f'orderbook_{market}'.lower()).replace('-','_')
            
            fields1 = ""
            for i in range(1, 11):
                fields1 += f"'bid_price_{i}', bid_price_{i}, 'bid_volume_{i}', bid_volume_{i}, 'ask_price_{i}', ask_price_{i}, 'ask_volume_{i}', ask_volume_{i}, "
            fields1 = fields1[:-2]

            fields2 = ""
            for i in range(11, 21):
                fields2 += f"'bid_price_{i}', bid_price_{i}, 'bid_volume_{i}', bid_volume_{i}, 'ask_price_{i}', ask_price_{i}, 'ask_volume_{i}', ask_volume_{i}, "
            fields2 = fields2[:-2]

            msg = f"""
                SELECT json_object_agg(to_char(date, 'YYYY-MM-DD HH24:MI:SSOF'), (
                    SELECT json_object_agg(key, value)
                    FROM (
                        SELECT *
                        FROM json_each(json_build_object({fields1}))
                        UNION ALL
                        SELECT *
                        FROM json_each(json_build_object({fields2}))
                    ) subq
                ))
                FROM {table_name}
                WHERE EXTRACT(epoch FROM date)::integer % {period} = 0
                AND date >= '{start}' AND date <= '{end}'
                GROUP BY date;
                """
            
            try:
                self.cur.execute(msg)
                data = [row[0] for row in self.cur.fetchall()]

            except (psycopg2.DatabaseError) as error:
                print(error)
                return data

            self.disconnect()

        return data


    ## ORDER IMBALANCE

    def create_order_imbalance_tables(self, markets_list):
        """
        Admin credentials required.
        """
        if not isinstance(markets_list, list):
            markets_list = [markets_list]

        self.connect()

        for market in markets_list:

            table_name = f'order_imbalance_{market}'.lower().replace('-','_')

            msg = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    date TIMESTAMP WITH TIME ZONE UNIQUE NOT NULL,
                    order_imbalance_bid REAL,
                    order_imbalance_ask REAL
                    );
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_oi_{market.lower().replace('-','_')} 
                    ON {table_name} (date);
                    GRANT SELECT ON TABLE {table_name} TO "read_only";
                    GRANT ALL ON TABLE {table_name} TO "developer";
                    GRANT ALL ON TABLE {table_name} TO "logger";
                    """
            try:
                self.cur.execute(msg)
                self.conn.commit()
            except (psycopg2.DatabaseError) as error:
                print(error)
                return False
            
        self.disconnect()
        return True
     
    def insert_order_imbalance_data(self, data_list, logger=None):    

        self.connect()

        if self.connected:
            for data in data_list:
                table_name = f'order_imbalance_{data["market"]}'.lower().replace('-','_')  

                msg_data = [data['date'], str(data['order_imbalance_bid']), str(data['order_imbalance_ask'])]

                args_str = ', '.join(['%s'] * len(msg_data))

                msg = f"""INSERT INTO {table_name} (
                    date, 
                    order_imbalance_bid, 
                    order_imbalance_ask
                ) VALUES ({args_str}) 
                ON CONFLICT (date) DO UPDATE 
                SET (order_imbalance_bid, order_imbalance_ask) = (EXCLUDED.order_imbalance_bid, EXCLUDED.order_imbalance_ask);"""

                try:
                    self.cur.execute(msg, msg_data)
                    self.conn.commit()

                except Exception as e:
                    if logger:
                        logger.error(e)
                        logger.error(f'Unable to save {data["market"]} data.')
                    else:
                        print(e)
                        print(f'Unable to save {data["market"]} data.')
                    return False

            self.disconnect()
            return True

        return False

    def get_order_imbalance_markets(self):
        """
        Returns a list of the order imbalance markets available.
        Empty list in case of any error.
        """
        market_list = []
        self.connect()

        if self.connected:
            msg = """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE';
                """

            try:
                self.cur.execute(msg)
                data = self.cur.fetchall()

                self.disconnect()

            except (psycopg2.DatabaseError) as error:
                print(error)
                return market_list
        
        filtered_list = [s for s in data if 'order_imbalance' in str(s)]
        
        for tables in filtered_list:
            market_list.append(tables[0].replace('order_imbalance_', '').replace('_', '-').upper())

        market_list.sort()

        return market_list

    def get_order_imbalance_market_info(self, markets_list):
        """
        Returns the date of the first record of order_imbalance for specific market or market list.
        """
        if not isinstance(markets_list, list):
            markets_list = [markets_list]

        market_info = {}

        self.connect()

        if self.connected:
            for market in markets_list:
                table_name = (f'order_imbalance_{market}'.lower()).replace('-','_')

                msg = f"""
                    SELECT date 
                    FROM {table_name}
                    ORDER BY date ASC
                    LIMIT 1;
                    """
                try:
                    self.cur.execute(msg)
                    data = self.cur.fetchall()

                except (psycopg2.DatabaseError) as error:
                    print(error)
                    return market_info
                
                market_info[market] = data[0][0]
            
            self.disconnect()

        return market_info

    def get_order_imbalance(self, market, start, end, period=60):
        """
        Returns the order imbalance data.

        Args: 
            market (str): desired market data.
            start, end (str): start and end date in format 'YYYY-MM-DD HH:MM:SS+00'
            period (int): number of seconds between samples.

        Returns:
            data -> A dict with the result of the query.
                Example:
                {target: {order_imbalance_bid : x, order_imbalance_ask : y}...}
        """

        period = int(period)

        data = {}

        self.connect()
        if self.connected:
            table_name = (f'order_imbalance_{market}'.lower()).replace('-','_')

            msg = f"""
                SELECT json_object_agg(to_char(date, 'YYYY-MM-DD HH24:MI:SSOF'), json_build_object(
                    'order_imbalance_bid', order_imbalance_bid,
                    'order_imbalance_ask', order_imbalance_ask
                ))
                FROM {table_name}
                WHERE EXTRACT(epoch FROM date)::integer % {period} = 0
                AND date >= '{start}' AND date <= '{end}'
                GROUP BY date;
                """
            
            try:
                self.cur.execute(msg)
                data = [row[0] for row in self.cur.fetchall()]

            except (psycopg2.DatabaseError) as error:
                print(error)
                return data

            self.disconnect()

        return data

    # Binance Candles Data
    def create_binance_candles_table(self):
        """
        Admin credentials required.
        """

        self.connect()

        table_name = f'binance_candles'

        header_base = ['open_price','high_price','low_price','close_price','volume','quoteassetvolume','numberoftrades','takerbuybaseassetvolume','takerbuyquoteassetvolume']
        header = ', '.join([f"{col} double precision" for col in header_base])

        msg = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    date TIMESTAMP WITH TIME ZONE NOT NULL,
                    updated TIMESTAMP WITH TIME ZONE NOT NULL,
                    market TEXT NOT NULL,
                    resolution TEXT NOT NULL,
                    {header}
                );

                CREATE INDEX IF NOT EXISTS idx_date_market_{table_name} 
                ON {table_name} (date,market);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_date_market_resolution_{table_name} 
                ON {table_name} (date,market,resolution);
                GRANT SELECT ON TABLE {table_name} TO "read_only";
                GRANT ALL ON TABLE {table_name} TO "developer";
                GRANT ALL ON TABLE {table_name} TO "logger";
                """

        try:
            self.cur.execute(msg)
            self.conn.commit()
        except (psycopg2.DatabaseError) as error:
            print('Unable to create candles table')
            print(error)
            return False
            
        self.disconnect()
        return True
    
    def insert_binance_candles_data(self, data_list, chunks , logger=None):

        self.connect()

        if self.connected:
            tuples = []
            for data in data_list:
                
                msg_data = tuple([data['startedAt'],
                            datetime.now(pytz.timezone('utc')), 
                            data['market'],
                            data['resolution'],
                            data['open'], 
                            data['high'], 
                            data['low'], 
                            data['close'], 
                            data['volume'],
                            data['QuoteAssetVolume'],
                            data['NumberOfTrades'],
                            data['TakerBuyBaseAssetVolume'],
                            data['TakerBuyQuoteAssetVolume']])
                
                tuples.append(msg_data)


            for data in self.splitList(tuples,chunks):
            
                values = [self.cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in data]

                args_str = ', '.join(values)
                
                table_name = f'binance_candles'

                msg = f"""INSERT INTO {table_name} (
                    date, 
                    updated,
                    market,
                    resolution,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    quoteassetvolume,
                    numberoftrades,
                    takerbuybaseassetvolume,
                    takerbuyquoteassetvolume
                ) VALUES {args_str}
                ON CONFLICT (date,market,resolution) DO UPDATE 
                SET (updated,open_price,high_price,low_price,close_price,volume,quoteassetvolume,numberoftrades,takerbuybaseassetvolume,takerbuyquoteassetvolume) 
                = (EXCLUDED.updated,EXCLUDED.open_price,EXCLUDED.high_price,EXCLUDED.low_price,EXCLUDED.close_price,EXCLUDED.volume,EXCLUDED.quoteassetvolume,EXCLUDED.numberoftrades,EXCLUDED.takerbuybaseassetvolume,EXCLUDED.takerbuyquoteassetvolume);"""
                
                try:
                    self.cur.execute(msg)
                    self.conn.commit()

                except Exception as e:
                    if logger:
                        logger.error(e)
                        logger.error(f'Unable to save {data["market"]} data.')
                    else:
                        print(e)
                        print(f'Unable to save {data["market"]} data.')
                    return False

            self.disconnect()
            return True

        return False
    
    def splitList(self,inputList, chunkSize):
        if chunkSize == 0:
            yield inputList
        else:
            for i in range(0, len(inputList), chunkSize):
                yield inputList[i:i + chunkSize]
                

    def get_binance_candle_data(self, market, start, end, resolution= '1HOUR',):
        """
        Returns the Binance candle data from db

        Args: 
            market (str): desired market data.
            start, end (str): start and end date in format 'YYYY-MM-DD HH:MM:SS+00'

        Returns:
            data -> A dict with the result of the query.
                Example:
                {date: ,market: ,open_price:, high_price:...}
        """

        data = {}

        self.connect()
        if self.connected:
            table_name = f'binance_candles'

            msg = f"""
                SELECT  json_build_object('market',market,
                    'date', to_char(date, 'YYYY-MM-DD HH24:MI:SSOF'), 
                    'open_price', open_price,
                    'high_price', high_price,
                    'low_price', low_price,
                    'close_price', close_price,
                    'volume', volume,
                    'QuoteAssetVolume',quoteassetvolume,
                    'NumberOfTrades',numberoftrades,
                    'TakerBuyBaseAssetVolume',takerbuybaseassetvolume,
                    'TakerBuyQuoteAssetVolume',takerbuyquoteassetvolume

                )
                FROM {table_name}
                WHERE  date >= '{start}' AND date < '{end}' and market = '{market}' and resolution = '{resolution}'
                """
            
            try:
                self.cur.execute(msg)
                raw_data = self.cur.fetchall()
                data = [row[0] for row in raw_data]

            except (psycopg2.DatabaseError) as error:
                print(error)
                return data

            self.disconnect()

        return data
    
    # dydx funding rate Data
    def create_dydx_funding_rate_table(self):
        """
        Admin credentials required.
        """

        self.connect()

        table_name = f'dydx_funding_rates'

        msg = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                date TIMESTAMP WITH TIME ZONE NOT NULL,
                market TEXT NOT NULL,
                funding_rate DOUBLE PRECISION,
                price DOUBLE PRECISION
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_{table_name} 
                ON {table_name} (date,market);
                GRANT SELECT ON TABLE {table_name} TO "read_only";
                GRANT ALL ON TABLE {table_name} TO "developer";
                GRANT ALL ON TABLE {table_name} TO "logger";
                """
        try:
            self.cur.execute(msg)
            self.conn.commit()
        except (psycopg2.DatabaseError) as error:
            print(error)
            return False
        
        self.disconnect()
        return True

    def insert_dydx_funding_rate_data(self, data_list,chunks,logger=None):

        self.connect()

        if self.connected:

            tuples = []

            for data in data_list:
                
                msg_data = [data['effectiveAt'],
                            data['market'],
                            data['rate'], 
                            data['price']]
                
                tuples.append(msg_data)

            table_name = f'dydx_funding_rates'

            for data in self.splitList(tuples,chunks):

                values = [self.cur.mogrify("(%s,%s,%s,%s)", tup).decode('utf8') for tup in data]

                args_str = ', '.join(values)

                msg = f"""INSERT INTO {table_name} (
                    date, 
                    market,
                    funding_rate,
                    price
                ) VALUES {args_str}
                ON CONFLICT (date,market) DO UPDATE 
                SET (funding_rate,price) 
                = (EXCLUDED.funding_rate,EXCLUDED.price);"""
                
                try:
                    self.cur.execute(msg, msg_data)
                    self.conn.commit()

                except Exception as e:
                    if logger:
                        logger.error(e)
                        logger.error(f'Unable to save {data["market"]} data.')
                    else:
                        print(e)
                        print(f'Unable to save {data["market"]} data.')
                    return False

            self.disconnect()
            return True

        return False

    def get_dydx_funding_rate_data(self,market,start,end):
        """
         Returns the dydx funding rate data from db

        Args: 
            market (str): desired market data.
            start, end (str): start and end date in format 'YYYY-MM-DD HH:MM:SS+00'

        Returns:
            data -> A dict with the result of the query.
                Example:
                {date: ,market:, funding_rate:...}       
        
        """
        data = {}

        self.connect()
        if self.connected:
            table_name = f'dydx_funding_rates'

            msg = f"""
                SELECT  json_build_object('market',market,
                    'date', to_char(date, 'YYYY-MM-DD HH24:MI:SSOF'), 
                    'funding_rate', funding_rate,
                    'price', price
                )
                FROM {table_name}
                WHERE  date >= '{start}' AND date < '{end}' and market = '{market}'
                """
            try:
                self.cur.execute(msg)
                data = [row[0] for row in self.cur.fetchall()]

            except (psycopg2.DatabaseError) as error:
                print(error)
                return data

            self.disconnect()

        return data

    # binance funding rate Data
    def create_binance_funding_rate_table(self):
        """
        Admin credentials required.
        """

        self.connect()

        table_name = f'binance_funding_rates'

        msg = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                date TIMESTAMP WITH TIME ZONE NOT NULL,
                market TEXT NOT NULL,
                funding_rate DOUBLE PRECISION
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_{table_name}
                ON {table_name} (date,market);
                GRANT SELECT ON TABLE {table_name} TO "read_only";
                GRANT ALL ON TABLE {table_name} TO "developer";
                GRANT ALL ON TABLE {table_name} TO "logger";
                """
        try:
            self.cur.execute(msg)
            self.conn.commit()
        except (psycopg2.DatabaseError) as error:
            print(error)
            return False
            
        self.disconnect()
        return True

    def insert_binance_funding_rate_data(self, data_list,chunks, logger=None):

        self.connect()

        if self.connected:

            tuples = []

            for data in data_list:

                msg_data = tuple([data['fundingTime'],
                            data['symbol'],
                            data['fundingRate']])
                
                tuples.append(msg_data)

            table_name = f'binance_funding_rates'

            for data in self.splitList(tuples,chunks):

                values = [self.cur.mogrify("(%s,%s,%s)", tup).decode('utf8') for tup in data]

                args_str = ', '.join(values)

                msg = f"""INSERT INTO {table_name} (
                    date, 
                    market,
                    funding_rate
                ) VALUES {args_str}
                ON CONFLICT (date,market) DO UPDATE 
                SET funding_rate = EXCLUDED.funding_rate;"""
                
                try:
                    self.cur.execute(msg, msg_data)
                    self.conn.commit()

                except Exception as e:
                    if logger:
                        logger.error(e)
                        logger.error(f'Unable to save {data["symbol"]} data.')
                    else:
                        print(e)
                        print(f'Unable to save {data["symbol"]} data.')
                    return False

            self.disconnect()
            return True

        return False

    def get_binance_funding_rate_data(self,market,start,end):
        """
         Returns the binance funding rate data from db

        Args: 
            market (str): desired market data.
            start, end (str): start and end date in format 'YYYY-MM-DD HH:MM:SS+00'

        Returns:
            data -> A dict with the result of the query.
                Example:
                {date: ,market:, funding_rate:...}       
        
        """
        data = {}

        self.connect()
        if self.connected:
            table_name = f'binance_funding_rates'

            msg = f"""
                SELECT  json_build_object('market',market,
                    'date', to_char(date, 'YYYY-MM-DD HH24:MI:SSOF'), 
                    'funding_rate', funding_rate
                )
                FROM {table_name}
                WHERE  date >= '{start}' AND date < '{end}' and market = '{market}'
                """
            try:
                self.cur.execute(msg)
                data = [row[0] for row in self.cur.fetchall()]

            except (psycopg2.DatabaseError) as error:
                print(error)
                return data

            self.disconnect()

        return data

def main():
    pass


if __name__ == '__main__':
    main()
