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
        klines = self.get_kline(**kwargs)
        if not len(klines):
            return

        current_dir = "."
        output_dir = os.path.join(current_dir, "output")
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        csv_file = os.path.join(output_dir, "kline.csv")

        columns = ["id", "open", "close", "low", "high", "amount", "vol", "count"]
        output_data = pandas.DataFrame(columns=columns)
        output_data = output_data.append(pandas.DataFrame(columns=columns, data=klines.get("data")), ignore_index=True)
        output_data.to_csv(csv_file, index=False, mode='w')

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
