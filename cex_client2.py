# -*- coding: utf-8 -*-
import logging
import hmac
import hashlib
import requests
import six


log = logging.getLogger(__name__)

import sys
import time


class CexClient(object):
    base_url = "https://cex.io/api/"

    def __init__(self, username, api_key, api_secret, timeout=None):
        self.__username = username
        self.__api_key = api_key
        self.__api_secret = api_secret
        self.__timeout = timeout
        self.__nonce_v = ''

    def __nonce(self):
        self.__nonce_v = '{:.10f}'.format(time.time() * 1000).split('.')[0]

    def __signature(self):
        if six.PY2:
            string = str(self.__nonce_v) + self.__username + self.__api_key  ##create string
            signature = hmac.new(self.__api_secret, string, digestmod=hashlib.sha256).hexdigest().upper()  ##create signature
        else:
            string = str(self.__nonce_v) + self.__username + self.__api_key  ##create string
            string_bytes = string.encode("utf8")
            signature = hmac.new(self.__api_secret.encode("utf8"), string_bytes, digestmod=hashlib.sha256).hexdigest().upper()  ##create signature
        return signature

    def __execute_request(self, url, params, http_method='GET'):
        content_type = 'application/json'
        http_headers = {'User-agent': 'client-cex.io-' + self.__username, 'Content-Type': content_type}

        result = {}
        response ={}
        try:
            prms = params if http_method == 'GET' else None
            data = params if http_method == 'POST' else None
            if self.__timeout is None:
                response = requests.request(http_method, url, params=prms, json=data, headers=http_headers, verify=False)
            else:
                response = requests.request(http_method, url, params=prms, json=data, headers=http_headers,
                                            verify=False, timeout=self.__timeout)
            result = response.json()
        except Exception as e:
            print(e)
            log.exception("Error while executing CEX request %s : %s %s" % (url, str(sys.exc_info()[0]), str(response)))

        return result

    def api_call(self, method, params={}, private=0, pair='', http_method=None):
        url = self.base_url + method + '/'

        if pair != '':
            url = url + pair + '/'

        if private == 1:  # add auth-data for non-public/private resources
            self.__nonce()
            params.update({'key': self.__api_key, 'signature': self.__signature(), 'nonce': self.__nonce_v})

        if http_method is None:
            http_method = 'POST' if private == 1 else 'GET'

        return self.__execute_request(url, params, http_method)

    def currency_limits(self):
        return self.api_call('currency_limits', {}, 0, '')

    def ticker(self, pair='BTC/USD'):
        return self.api_call('ticker', {}, 0, pair)

    def tickers_for_all_pairs_by_markets(self, markets="BTC/USD"):
        return self.api_call('tickers', {}, 0, markets)

    def last_price(self, pair="BTC/USD"):
        return self.api_call('last_price', {}, 0, pair)

    def last_prices_for_given_markets(self, markets="BTC/USD"):
        return self.api_call('last_prices', {}, 0, markets)

    def converter(self, pair="BTC/USD", amount=1):
        return self.api_call('convert', {"amnt": amount}, 1, pair)

    def chart(self, pair='BTC/USD', last_hours="24", max_resp_arr_size=100):
        return self.api_call('price_stats', {"lastHours": last_hours, "maxRespArrSize": max_resp_arr_size}, 1, pair)

    def historical_1m_ohlcv(self, pair='BTC/USD', date="20170925"):
        method = "ohlcv/hd/{}/".format(date)
        return self.api_call(method, {}, 0, pair)

    def order_book(self, pair='BTC/USD', depth=None):
        params = {"depth": depth} if depth is not None else {}
        return self.api_call('order_book', params, 0, pair)

    def trade_history(self, pair='BTC/USD', since=1):
        return self.api_call('trade_history', {"since": str(since)}, 0, pair)

    def balance(self):
        return self.api_call('balance', {}, 1)

    def current_orders(self, pair='BTC/USD'):
        return self.api_call('open_orders', {}, 1, pair)

    def status_order(self, order_id):
        return self.api_call('get_order', {"id": order_id}, 1)

    def cancel_order(self, order_id, label="deflabel"):
        return self.api_call('cancel_order', {"id": order_id}, 1)

    def cancel_all(self, pair='BTC/USD'):
        return self.api_call('cancel_orders', {}, 1, pair)

    def get_order(self, order_id):
        return self.api_call('get_order', {"id": order_id}, 1)

    def get_order_tx(self, order_id):
        return self.api_call('get_order_tx', {"id": order_id}, 1)

    def place_order(self, op='buy', amount=1, price=1, pair='BTC/USD'):
        return self.api_call('place_order', {"type": op, "amount": amount, "price": price}, 1, pair)

    def place_market_order(self, op='buy', amount=1, pair='BTC/USD'):
        return self.api_call('place_order', {"type": op, "amount": amount, "order_type": "market"}, 1, pair)

    def archived_orders(self, pair='BTC/USD', dfrom=int(time.time()) - 84600, dto=int(time.time()), limit='100',
                        status=None, lastTxDateFrom=None, lastTxDateTo=None):
        params = {
            "dateFrom": dfrom,
            "dateTo": dto,
            "limit": limit
        }

        if lastTxDateFrom is not None:
            params["lastTxDateFrom"] = lastTxDateFrom

        if lastTxDateTo is not None:
            params["lastTxDateTo"] = lastTxDateTo

        if status is not None:
            params["status"] = status
        return self.api_call('archived_orders', params, 1, pair)

    def archived_orders_lasttx(self, pair='BTC/USD', dfrom=None, dto=None, lastFrom=int(time.time()) - 3600,
                               lastTo=int(time.time()), limit='100', status=None):
        params = {
            "dateFrom": dfrom,
            "dateTo": dto,
            "limit": limit,
            "lastTxDateFrom": lastFrom,
            "lastTxDateTo": lastTo
        }

        if status is not None:
            params["status"] = status
        return self.api_call('archived_orders', params, 1, pair)

    def get_fee(self):
        return self.api_call('get_myfee', {}, 1)

    def cancel_replace_order(self, pair='BTC/USD', op='buy', amount=0.0, price=0.0, order_id=0):
        return self.api_call('cancel_replace_order', {"order_id": str(order_id), "type": op, "amount": str(amount), "price": str(price)}, 1, pair)

    def orders_active_status(self, orders_list):
        return self.api_call('active_orders_status', {"orders_list": orders_list}, 1)

    def ohlcv_new(self, pair, date_str):
        path = 'ohlcv/hd/%s/%s' % (date_str, pair)
        url = self.base_url + path
        return self.__execute_request(url, {})


if __name__ == "__main__":
    # Check comments above before running.
    user = ""
    key = ""
    secret = ""

    api = CexClient(username=user, api_key=key, api_secret=secret)

    currency_limits = api.currency_limits()
    print("Currency limits: %s" % str(currency_limits)[:200])

    ticker = api.ticker("BTC/USD")
    print("Ticker: %s" % ticker)

    tickers_for_pairs = api.tickers_for_all_pairs_by_markets("BTC/USD")
    print("Tickers for pairs: %s" % str(tickers_for_pairs)[:200])

    last_price = api.last_price("BTC/USD")
    print("Last price: %s" % last_price)

    last_prices = api.last_prices_for_given_markets("BTC/USD")
    print("Last prices: %s" % str(last_prices)[:200])

    converted = api.converter("BTC/USD", 1)
    print("Converted: %s" % converted)

    chart = api.chart("BTC/USD", 10, 20)
    print("Chart: %s" % str(chart)[:200])

    historical_1m_ohlcv = api.historical_1m_ohlcv("20180913", "BTC/USD")
    print("Historical 1m OHLCV: %s" % str(historical_1m_ohlcv)[:200])

    orderbook = api.order_book("BTC/USD", depth=3)
    print("Orderbook: %s" % orderbook)

    trade_history = api.trade_history("BTC/USD")
    print("Trade history: %s" % str(trade_history)[:200])

    import os
    os._exit(0)
    # WARNING: orders placements above. Check before run!
    balance = api.balance()
    print("Balance: %s" % balance)

    open_orders = api.current_orders("BTC/USD")
    print("Open orders: %s" % open_orders)

    order = api.place_order(pair="BTC/USD", op="sell", price=20000, amount=0.002)
    print("Order: %s" % order)

    open_orders = api.current_orders("BTC/USD")
    print("Open orders: %s" % open_orders)

    open_orders_by_pair = api.current_orders("BTC/USD")
    print("Open orders by pair: %s" % open_orders_by_pair)

    active_orders_status = api.orders_active_status([order["id"]])
    print("Active orders status: %s" % active_orders_status)

    order_details = api.get_order(order["id"])
    print("Order details: %s" % order_details)

    canceled = api.cancel_order(order["id"])
    print("Canceled: %s" % canceled)

    order = api.place_order(pair="BTC/USD", op="sell", price=20000, amount=0.002)
    print("Another order: %s" % order)

    replaced_order = api.cancel_replace_order(pair="BTC/USD", op="sell", amount=0.002, price=19999, order_id=order["id"])
    print("Replaced order: %s" % replaced_order)

    canceled_by_pair = api.cancel_all("BTC/USD")
    print("Canceled by pair: %s" % canceled_by_pair)

    open_orders = api.current_orders("BTC/USD")
    print("Open orders: %s" % open_orders)

    archived_orders = api.archived_orders("BTC/USD")
    print("Archived orders: %s" % archived_orders[-1])

    order_tx = api.get_order_tx(order["id"])
    print("Order transactions: %s" % order_tx)

    my_fee = api.get_fee()
    print("My fee: %s" % my_fee)

    # WARNING, not tested, it's only an idea of stop-loss and take-profit implementation.
    # Use at your own risk.
    def stop_loss_take_profit_timeout_order(pair, op, amount, stop_loss_price, take_profit_price, time_live=None):
        order = api.place_market_order(pair=pair, amount=amount, op=op)
        open_price = float(order["price"])
        placed_at = int(int(order["time"]) / 1000)

        opposite_order = None
        while True:
            time.sleep(1)
            now = int(time.time())
            ticker = api.ticker(pair)
            last_price = float(ticker["last"])

            if op == "buy":
                if last_price >= take_profit_price or last_price <= stop_loss_price:
                    opposite_order = api.place_market_order(pair=pair, amount=amount, op="sell")
                    break
            elif op == "sell":
                if last_price <= take_profit_price or last_price >= stop_loss_price:
                    opposite_order = api.place_market_order(pair=pair, amount=amount, op="buy")
                    break

            if now - placed_at >= time_live:
                opposite_order = api.place_market_order(pair=pair, amount=amount, op="buy")
                break

        close_price = opposite_order["price"]
        print("%s: %s %s at %s, %s at %s." % (pair, order["type"], amount, open_price,
                                              opposite_order["type"], close_price))
        return
