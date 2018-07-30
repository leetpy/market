#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests


class Broker(object):
    
    def __init__(self):
        pass

    def _request_api(self, method, path, data, headers, **kwargs):
        pass
    
    def _get(self, url, params, add_to_headers=None):
        headers = {
            "Content-type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
        }
        if add_to_headers:
            headers.update(add_to_headers)
        response = self._request_api("get", url, params, headers)


    def _post(self, url, params, add_to_headers=None):
        pass

    def get_kline(self, symbol, period, size=150):
        raise NotImplementedError()

    def get_depth(self, symbol, type):
        raise NotImplementedError()

    def get_trade(self, symbol):
        raise NotImplementedError()

    def get_ticker(self, symbol):
        raise NotImplementedError()
