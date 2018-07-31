#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas
import os
from market import base


class HBBroker(base.Broker):
    
    MARKET_URL = "https://api.hadax.com"

    def __init__(self):
        super(HBBroker, self).__init__()

    def get_kline(self, **kwargs):
        uri = os.path.join(self.MARKET_URL, 'market/history/kline')
        return self._get(uri, data=kwargs)        

    def kline_to_csv(self, **kwargs):
        klines = self.get_kline(kwargs)
        current_dir = "."
        output_dir = os.path.join(current_dir, "output")
        os.makedirs(output_dir)
        columns = ["id", "open", "close", "low", "high", "amount", "vol", "count"]
        output_data = pandas.DataFrame(columns=columns)
        while True:
            try:
                tmp_data = self.get_kline(kwargs)
            except Exception as e:
                print(e)

            if not len(tmp_data):
                break

            output_data = output_data.append(pd.DataFrame(columns=output_data.columns, data=tmp_data),
                                             ignore_index = True)

        return output_data

    def get_symbols(self):
        uri = os.path.join(self.MARKET_URL, "v1/common/symbols")
        return self._get(uri, data={})

    def get_trade(self, symbol):
        uri = os.path.join(self.MARKET_URL, "market/trade")
        data = {"symbol": symbol}
        return self._get(uri, data=data)

    def _is_valid_symbol(self, symbol):
        if symbol in ["ethbtc", "ltcbtc", "etcbtc", "bchbtc"]:
            return True
        return False
