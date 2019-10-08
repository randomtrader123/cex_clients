import hashlib
import hmac
import time
import websocket
import logging
import ssl
import six
from json import dumps, loads
from functools import partial
from datetime import datetime as dt
from threading import Thread


logging.basicConfig()


def utc_timestamp():
    dt1 = dt.utcnow()
    epoch = dt.utcfromtimestamp(0)
    now = int((dt1 - epoch).total_seconds())
    return now


def auth_required(func):
    def wrapper(self, *args, **kwargs):
        if self.is_authenticated is True:
            return func(self, *args, **kwargs)
        else:
            print("Auth required to send %s request!" % func.__name__)

    return wrapper


class CexWsClient(object):
    url = "wss://ws.cex.io/ws"

    def __init__(self, user, key, secret):
        self.user = user
        self.key = key
        self.secret = secret
        self.stop_flag = False
        self.main_thread = None
        self.ws_thread = None
        self.connection = None
        self.connection_thread = None
        self.is_authenticated = False

    def nonce(self):
        return str(utc_timestamp())

    def signature(self, nonce):
        string = str(nonce) + self.key
        if six.PY2:
            signature = hmac.new(self.secret, string, digestmod=hashlib.sha256).hexdigest().upper()
        else:
            string_bytes = string.encode("utf8")
            signature = hmac.new(self.secret.encode("utf8"), string_bytes, digestmod=hashlib.sha256).hexdigest().upper()
        return signature

    def get_oid(self, method):
        return "%s_%s" % (method, int(time.time()))

    def authenticate(self):
        nonce = int(time.time())
        msg = {
            "e": "auth",
            "auth": {
                "key": self.key,
                "signature": self.signature(nonce),
                "timestamp": nonce
            }
        }

        return self.send_message(msg)

    def subscribe_to_tickers(self):
        msg = {
            "e": "subscribe",
            "rooms": [
                "tickers"
            ]
        }

        return self.send_message(msg)

    def subscribe_to_ohlcv(self, pair, timeframe):
        s1, s2 = pair.split("/")
        msg = {
            "e": "init-ohlcv",
            "i": timeframe,
            "rooms": [
                "pair-%s-%s" % (s1, s2)
            ]
        }

        return self.send_message(msg)

    def subscribe_to_old_pair_room(self, pair):
        s1, s2 = pair.split("/")
        msg = {
            "e": "subscribe",
            "rooms": ["pair-%s-%s" % (s1, s2)]
        }

        return self.send_message(msg)

    @auth_required
    def get_ticker(self, pair):
        s1, s2 = pair.split("/")
        msg = {
            "e": "ticker",
            "data": [s1, s2],
            "oid": self.get_oid("%s_ticker" % pair)
        }
        return self.send_message(msg)

    @auth_required
    def get_balance(self):
        msg = {
            "e": "get-balance",
            "oid": self.get_oid("get-balance")
        }
        return self.send_message(msg)

    @auth_required
    def subscribe_to_order_book(self, pair, depth, subscribe=True):
        s1, s2 = pair.split("/")
        msg = {
            "e": "order-book-subscribe",
            "data": {
                "pair": [
                    s1, s2
                ],
                "subscribe": subscribe,
                "depth": depth
            },
            "oid": self.get_oid("%s-%s-md-subscr" % (pair, depth))
        }

        return self.send_message(msg)

    @auth_required
    def unsubscribe_from_order_book(self, pair):
        s1, s2 = pair.split("/")
        msg = {
            "e": "order-book-unsubscribe",
            "data": {
                "pair": [
                    s1, s2
                ]
            },
            "oid": self.get_oid("%s-md-unsubscr" % pair)
        }

        return self.send_message(msg)

    @auth_required
    def open_orders(self, pair):
        s1, s2 = pair.split("/")
        msg = {
            "e": "open-orders",
            "data": {
                "pair": [
                    s1, s2
                ]
            },
            "oid": self.get_oid("%s-open-orders" % pair)
        }

        return self.send_message(msg)

    @auth_required
    def place_order(self, pair, op, price, amount):
        s1, s2 = pair.split("/")
        msg = {
            "e": "place-order",
            "data": {
                "pair": [
                    s1,
                    s2
                ],
                "amount": str(amount),
                "price": str(price),
                "type": op
            },
            "oid": self.get_oid("%s-%s-%s-%s-place-order" % (pair, op, price, amount))
        }

        return self.send_message(msg)

    @auth_required
    def cancel_replace_order(self, order_id, pair, op, price, amount):
        s1, s2 = pair.split("/")
        msg = {
            "e": "cancel-replace-order",
            "data": {
                "order_id": order_id,
                "pair": [
                    s1, s2
                ],
                "amount": str(amount),
                "price": str(price),
                "type": op
            },
            "oid": self.get_oid("%s-%s-%s-%s-%s-cancel-replace-order" % (order_id, pair, op, price, amount))
        }

        return self.send_message(msg)

    @auth_required
    def get_order(self, order_id):
        msg = {
            "e": "get-order",
            "data": {
                "order_id": order_id,
            },
            "oid": self.get_oid("%s-get-order" % order_id)
        }

        return self.send_message(msg)

    @auth_required
    def cancel_order(self, order_id):
        msg = {
            "e": "cancel-order",
            "data": {
                "order_id": order_id,
            },
            "oid": self.get_oid("%s-cancel-order" % order_id)
        }

        return self.send_message(msg)

    @auth_required
    def archived_orders(self, pair, date_from=None, date_to=None, limit=100):
        s1, s2 = pair.split("/")
        msg = {
            "e": "archived-orders",
            "data": {
                "pair": [
                    s1, s2
                ],
                "limit": limit
            },
            "oid": self.get_oid("%s-%s-archived-orders")
        }

        if date_from is not None:
            msg["data"]["dateFrom"] = date_from

        if date_to is not None:
            msg["data"]["dateTo"] = date_to

        return self.send_message(msg)

    def send_message(self, message):
        self.connection.send(dumps(message))
        oid = message.get("oid", None)
        print("Sent %s" % message)
        return oid

    def on_open_py3(self):
        return self.on_open(self.connection)

    def on_message_py3(self, message):
        return self.on_message(self.connection, message)

    def on_close_py3(self):
        return self.on_close(self.connection)

    def on_error_py3(self, error):
        return self.on_error(self.connection, error)

    def on_open(self, ws):
        print("Opened WebSocket connection to %s" % self.url)
        self.is_authenticated = False
        self.authenticate()

    def on_message(self, ws, message):
        message = loads(message)
        e = message.get("e", None)

        if e == "ping":
            self.send_message({"e": "pong"})

        elif e == "auth":
            if message["ok"] == "ok":
                self.is_authenticated = True
                print("Successfuly authenticated!")
            else:
                print("Not authenticated: %s" % message)

        elif e == "tick":
            print("Got tick: %s" % message)

        elif e.startswith("ohlcv"):
            print("Got ohlcv subscription message: %s" % str(message)[:200])

        elif e == "md":
            print("Got order book snapshot: %s" % str(message)[:200])

        elif e == "md_grouped":
            print("Got grouped md: %s" % str(message)[:200])

        elif e == "history":
            print("Got trade history: %s" % str(message)[:200])

        elif e == "history-update":
            print("Got trade history update: %s" % str(message)[:200])

        elif e == "order-book-subscribe":
            print("Gor %s: %s" % (e, message))

        elif e == "order-book-unsubscribe":
            print("Unsubscribed from order book: %s" % message)

        elif e == "open-orders":
            print("Open orders: %s" % message)

        elif e == "place-order":
            print("Placed order: %s" % message)

        elif e == "cancel-replace-order":
            print("Replaced order: %s" % message)

        elif e == "cancel-order":
            print("Canceled order: %s" % message)

        elif e == "ticker":
            print("Got ticker: %s" % message)

        elif e == "get-balance":
            print("Got balance: %s" % message)

        elif e == "get-order":
            print("Got order: %s" % message)

        elif e == "archived-orders":
            print("Archived orders: %s" % str(message)[:300])

        elif e == "tx":
            print("Transaction created: %s" % message)

        elif e == "balance":
            print("Balance: %s" % message)

        elif e == "obalance":
            print("Obalance: %s" % message)

        elif e == "md_update":
            print("Got MD update: %s" % message)

        elif e == "order":
            print("Order change: %s" % message)
        else:
            print("Got message %s: %s" % (e, str(message)[:200]))

    def on_close(self, ws):
        print("Closed WebSocket connection to %s" % self.url)
        self.connection = None
        time.sleep(5)
        if self.stop_flag is False:
            self.connect_and_run()
        else:
            print("Stop flag is True, won't reconnect.")

    def on_error(self, ws, error):
        print("Error in WebSocket connection to %s: %s" % (self.url, error))
        self.stop()
        time.sleep(5)
        self.stop_flag = False
        self.connect_and_run()

    def main_loop_function(self):
        self.connect_and_run()
        while self.stop_flag is False:
            time.sleep(10)

        self.connection.close()
        return

    def connect_and_run(self):
        if self.connection is None:
            headers = {
                'Accept-Language: en-us',
                'Keep-Alive: 30',
                'Cache-Control: max-age=0',
                'Connection: keep-alive',
                'Sec-WebSocket-Extensions: deflate-frame'
            }

            websocket._default_timeout = 30
            websocket.enableTrace(False)
            if six.PY2:
                self.connection = websocket.WebSocketApp(self.url, header=headers, on_open=self.on_open,
                                                         on_close=self.on_close, on_message=self.on_message,
                                                         on_error=self.on_error)
            else:
                self.connection = websocket.WebSocketApp(self.url, header=headers, on_open=self.on_open_py3,
                                                         on_close=self.on_close_py3, on_message=self.on_message_py3,
                                                         on_error=self.on_error_py3)
            prt = partial(self.connection.run_forever, **{"sslopt": {"cert_reqs": ssl.CERT_NONE},
                                                          "origin": "https://cex.io/api"})
            self.connection_thread = Thread(target=prt, name="WSCONN")
            self.connection_thread.start()

    def stop(self):
        print("About to stop websocket...")
        self.stop_flag = True
        if self.main_thread.is_alive():
            self.main_thread.join()

    def start(self):
        self.stop_flag = False
        self.main_thread = Thread(target=self.main_loop_function, name="CEXWS")
        self.main_thread.start()
        time.sleep(10)


if __name__ == "__main__":
    user = ""
    key = ""
    secret = ""

    ws_cli = CexWsClient(user, key, secret)
    ws_cli.start()

    # ws_cli.subscribe_to_tickers()
    # ws_cli.subscribe_to_ohlcv("BTC/USD", "1h")
    # ws_cli.subscribe_to_order_book("BTC/USD", 3)
    # ws_cli.subscribe_to_old_pair_room("BTC/USD")

    ws_cli.get_ticker("BTC/USD")
    ws_cli.get_balance()

    ws_cli.subscribe_to_order_book("BTC/USD", depth=5)
    time.sleep(10)
    ws_cli.unsubscribe_from_order_book("BTC/USD")

    # ws_cli.place_order(pair="BTC/USD", op="sell", price=20000, amount=0.002)
    # ws_cli.cancel_replace_order(order_id=10466806398, pair="BTC/USD", op="sell", price=19999, amount=0.002)
    # ws_cli.cancel_order(order_id=10466844257)
    # ws_cli.open_orders("BTC/USD")
    # ws_cli.get_order(order_id=10466844257)
    # ws_cli.archived_orders(pair="BTC/USD")

    time.sleep(5)
    ws_cli.stop()
