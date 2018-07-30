#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import time
from operator import itemgetter
import os
import pandas as pd

from market import base


class BaBroker(base.Broker):
    
    API_URL = 'https://api.binance.com/api'
    PUBLIC_API_VERSION = 'v1'

    def __init__(self, projectDir='/home/harlay/blockChain/CSVS', requests_params=None):
        self.session = self._init_session()
        self._requests_params = requests_params
        self.__projectDir = projectDir
        self.__csvfileFlag = False
        self.__exception = None
        # init DNS and SSL cert
        self.ping()

    def _init_session(self):
    
        session = requests.session()
        session.headers.update({'Accept': 'application/json',
                                'User-Agent': 'binance/python'})
        return session

    def _create_api_uri(self, path, signed=True, version=PUBLIC_API_VERSION):
        v = self.PRIVATE_API_VERSION if signed else version
        return self.API_URL + '/' + v + '/' + path


    def _order_params(self, data):
        """Convert params to list with signature as last element

        :param data:
        :return:

        """
        has_signature = False
        params = []
        for key, value in data.items():
            if key == 'signature':
                has_signature = True
            else:
                params.append((key, value))
        # sort parameters by key
        params.sort(key=itemgetter(0))
        if has_signature:
            params.append(('signature', data['signature']))
        return params

    def _request(self, method, uri, signed, force_params=False, **kwargs):

        # set default requests timeout
        kwargs['timeout'] = 30

        # add our global requests params
        if self._requests_params:
            kwargs.update(self._requests_params)

        data = kwargs.get('data', None)
        if data and isinstance(data, dict):
            kwargs['data'] = data
        if signed:
            # generate signature
            kwargs['data']['timestamp'] = int(time.time() * 1000)
            kwargs['data']['signature'] = self._generate_signature(kwargs['data'])

        # sort get and post params to match signature order
        if data:
            # find any requests params passed and apply them
            if 'requests_params' in kwargs['data']:
                # merge requests params into kwargs
                kwargs.update(kwargs['data']['requests_params'])
                del(kwargs['data']['requests_params'])

            # sort post params
            kwargs['data'] = self._order_params(kwargs['data'])

        # if get request assign data array to params value for requests lib
        if data and (method == 'get' or force_params):
            kwargs['params'] = kwargs['data']
            del(kwargs['data'])

        response = getattr(self.session, method)(uri, **kwargs)
        return self._handle_response(response)

    def _request_api(self, method, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        uri = self._create_api_uri(path, signed, version)
        return self._request(method, uri, signed, **kwargs)


    def _handle_response(self, response):
        if not str(response.status_code).startswith('2'):
            raise Exception(response)
        try:
            return response.json()
        except ValueError:
            raise Exception('Invalid Response: %s' % response.text)

    def _get(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        return self._request_api('get', path, signed, version, **kwargs)

    def _post(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        return self._request_api('post', path, signed, version, **kwargs)

    def _put(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        return self._request_api('put', path, signed, version, **kwargs)

    def _delete(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        return self._request_api('delete', path, signed, version, **kwargs)

    def get_exchange_info(self):
        return self._get('exchangeInfo')

    def get_symbols(self):
        res = self._get('exchangeInfo')
        return[ item['symbol'] for item in res['symbols']]

    def ping(self):
        return self._get('ping')


    def get_klines(self, **params):
        return self._get('klines', data=params)

    def _get_earliest_valid_timestamp(self, symbol, interval):
        kline = self.get_klines(
            symbol=symbol,
            interval=interval,
            limit=1,
            startTime=0,
            endTime=None
        )
        return kline[0][0]

    def _get_lastest_valid_timestamp_from_csv(self, symbol):
        csvfile = self._get_csv_file(symbol)
        if os.path.exists(csvfile):
            self.__csvfileFlag = True
            df = pd.read_csv(csvfile)
            timestamp = int(df.tail(1)['Close time'].tolist()[0]) + 1
        else:
            timestamp = 0
        return timestamp

    def generate_klines_to_csv(self, symbol):
        self.__csvfileFlag = False
        klines = self.get_historical_klines(symbol, "1m")
        if klines.empty:
            return
        #algoKlines = klines.loc[:, ['Open time', 'Open', 'Close','High', 'Low','Volume']]
        #algoKlines['Date Time'] = pd.to_datetime(algoKlines['Open time']/1000, unit = 's')
        #algoKlines.drop(['Open time'], axis = 1, inplace=True)
        #algoKlines.columns = ['Open', 'Close', 'High', 'Low', 'Volume', 'Date Time']
        if self.__csvfileFlag:
            klines.to_csv(self._get_csv_file(symbol), header=False, index = False, sep = ',', mode='a')
            #algoKlines.to_csv(self._get_algo_csv_file(), header=False, index = False, sep = ',', mode='a', columns = [ 'Date Time', 'Open', 'Close', 'High', 'Low', 'Volume'])
        else:
            klines.to_csv(self._get_csv_file(symbol), index = False, sep = ',', mode='w')
            #algoKlines.to_csv(self._get_algo_csv_file(), index = False, sep = ',', mode='a', columns = [ 'Date Time', 'Open', 'Close', 'High', 'Low', 'Volume'])
            self.__csvfileFlag = False
        if self.__exception:
            raise Exception(e)

    def _get_csv_file(self, symbol):
        return os.path.join(self.__projectDir, 'original', 'BA-' + symbol + '.csv')

    def get_historical_klines(self, symbol, interval, start_str=None, end_str=None):
        """Get Historical Klines from Binance

        See dateparser docs for valid start and end string formats http://dateparser.readthedocs.io/en/latest/

        If using offset strings for dates add "UTC" to date string e.g. "now UTC", "11 hours ago UTC"

        :param symbol: Name of symbol pair e.g BNBBTC
        :type symbol: str
        :param interval: Binance Kline interval
        :type interval: str
        :param start_str: Start date string in UTC format or timestamp in milliseconds
        :type start_str: str|int
        :param end_str: optional - end date string in UTC format or timestamp in milliseconds (default will fetch everything up to now)
        :type end_str: str|int

        :return: list of OHLCV values

        """
        # init our list
        #output_data = []
        output_data = pd.DataFrame(columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore'])

        # setup the max limit
        limit = 500

        # convert interval to useful value in seconds
        #timeframe = interval_to_milliseconds(interval)
        timeframe = 60000

        # convert our date strings to milliseconds
        #if type(start_str) == int:
        #    start_ts = start_str
        #else:
        #    start_ts = date_to_milliseconds(start_str)

        # establish first available start timestamp
        #first_valid_ts = max(self._get_earliest_valid_timestamp(symbol, interval), self._get_lastest_valid_timestamp_from_csv(symbol))
        #start_ts = max(start_ts, first_valid_ts)
        start_ts = max(self._get_earliest_valid_timestamp(symbol, interval), self._get_lastest_valid_timestamp_from_csv(symbol))

        # if an end time was passed convert it
        end_ts = None
        #if end_str:
        #    if type(end_str) == int:
        #        end_ts = end_str
        #    else:
        #        end_ts = date_to_milliseconds(end_str)

        idx = 0
        while True:
            # fetch the klines from start_ts up to max 500 entries or the end_ts if set
            try:
                temp_data = self.get_klines(
                    symbol=symbol,
                    interval=interval,
                    limit=limit,
                    startTime=start_ts,
                    endTime=end_ts
                )
            except Exception as e:
                print(e)
                self.__exception = e
                return output_data

            # handle the case where exactly the limit amount of data was returned last loop
            if not len(temp_data):
                break

            # append this loops data to our output data
            #output_data += temp_data
            output_data = output_data.append(pd.DataFrame(columns = output_data.columns, data = temp_data), ignore_index = True)

            # set our start timestamp using the last value in the array
            start_ts = temp_data[-1][0]

            idx += 1
            # check if we received less than the required limit and exit the loop
            if len(temp_data) < limit:
                # exit the while loop
                break

            # increment next call by our timeframe
            start_ts += timeframe

            # sleep after every 3rd call to be kind to the API
            if idx % 3 == 0:
                time.sleep(1)
                
            print("getting no. %d klines"%idx)

        return output_data