#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests


class Broker(object):
    
    def __init__(self):
        self.session = self._init_session()

    def _init_session(self):
        session = requests.session()
        return session

    def _request(self, method, uri, **kwargs):
        # set default requests timeout
        kwargs['timeout'] = 30
        
        headers = kwargs.get("headers")
        if headers:
            self.session.headers.update(headers)
        
        data = kwargs.get("data")
        if data and method == "get":
            kwargs["params"] = data
            del kwargs["data"]
        
        response = getattr(self.session, method)(uri, **kwargs)
        return self._handle_response(response)

    def _handle_response(self, response):
        if not str(response.status_code).startswith('2'):
            raise Exception(response)
        try:
            return response.json()
        except ValueError:
            raise Exception('Invalid Response: %s' % response.text)

    def _get(self, uri, **kwargs):
        return self._request("get", uri, **kwargs)

    def _post(self, url, **kwargs):
        return self._request("post", uri, **kwargs)

    def get_kline(self, **kwargs):
        raise NotImplementedError()

    def get_depth(self, symbol, type):
        raise NotImplementedError()

    def get_trade(self, symbol):
        raise NotImplementedError()

    def get_ticker(self, symbol):
        raise NotImplementedError()

    def get_symbols(self):
        raise NotImplementedError()
