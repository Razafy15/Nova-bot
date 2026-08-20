import os
import json
import time
import threading

import requests
import websocket

from flask import Flask, render_template, request, jsonify


app = Flask(__name__)


# =========================================================
# GLOBAL BOT STATE
# =========================================================

bot = {
    "app_id": "",
    "pat_token": "",
    "account_id": "",
    "symbol": "BOOM500",

    "connected": False,
    "connecting": False,

    "balance": 0.0,
    "currency": "USD",

    "price": 0.0,

    "chart": [],

    "message": "Miandry connexion...",
    "error": "",

    "last_update": 0,

    "ws": None,
}


ws_lock = threading.Lock()
stop_event = threading.Event()


# =========================================================
# HELPERS
# =========================================================

def log(message):
    print(
        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}",
        flush=True
    )


def set_message(message):
    bot["message"] = str(message)
    log(message)


def set_error(message):
    bot["error"] = str(message)
    bot["message"] = str(message)

    log("ERROR: " + str(message))


def clear_error():
    bot["error"] = ""


# =========================================================
# DERIV WEBSOCKET
# =========================================================

def send_ws(data):
    ws = bot.get("ws")

    if ws is None:
        return False

    try:
        with ws_lock:
            ws.send(json.dumps(data))

        return True

    except Exception as e:

        set_error(
            f"Tsy afaka nandefa WebSocket: {e}"
        )

        return False


# =========================================================
# AUTHENTICATION
# =========================================================

def authorize_deriv(ws):

    token = bot["pat_token"]

    if not token:

        set_error(
            "Tsy misy PAT Token."
        )

        return False

    log("Mandefa authorize amin'i Deriv...")

    try:

        ws.send(
            json.dumps(
                {
                    "authorize": token,
                    "req_id": 1
                }
            )
        )

        return True

    except Exception as e:

        set_error(
            f"Authorize error: {e}"
        )

        return False


# =========================================================
# MARKET DATA
# =========================================================

def request_balance():

    send_ws(
        {
            "balance": 1,
            "subscribe": 1,
            "req_id": 10
        }
    )


def request_chart():

    symbol = bot["symbol"]

    log(
        f"Mangataka chart: {symbol}"
    )

    send_ws(
        {
            "ticks_history": symbol,
            "adjust_start_time": 1,
            "count": 200,
            "end": "latest",
            "style": "candles",
            "granularity": 60,
            "subscribe": 1,
            "req_id": 20
        }
    )


def request_tick_stream():

    symbol = bot["symbol"]

    send_ws(
        {
            "ticks": symbol,
            "subscribe": 1,
            "req_id": 30
        }
    )


# =========================================================
# FORMAT CANDLES
# =========================================================

def process_history_candles(candles):

    formatted = []

    for candle in candles:

        try:

            item = {
                "time": int(candle["epoch"]),
                "open": float(candle["open"]),
                "high": float(candle["high"]),
                "low": float(candle["low"]),
                "close": float(candle["close"])
            }

            formatted.append(item)

        except Exception as e:

            log(
                f"Candle tsy mety: {e}"
            )


    # Lightweight Charts mila time miakatra
    formatted.sort(
        key=lambda x: x["time"]
    )


    # Esorina duplicate
    unique = []

    seen = set()

    for candle in formatted:

        if candle["time"] in seen:
            continue

        seen.add(candle["time"])

        unique.append(candle)


    bot["chart"] = unique[-300:]


    if bot["chart"]:

        bot["price"] = float(
            bot["chart"][-1]["close"]
        )

        bot["last_update"] = time.time()


# =========================================================
# LIVE TICK
# =========================================================

def process_tick(tick):

    try:

        quote = float(
            tick.get("quote", 0)
        )

        bot["price"] = quote
        bot["last_update"] = time.time()

    except Exception:
        pass


# =========================================================
# WEBSOCKET MESSAGE
# =========================================================

def on_message(ws, message):

    try:

        data = json.loads(message)

    except Exception as e:

        log(
            f"JSON error: {e}"
        )

        return


    # API error
    if "error" in data:

        error = data["error"]

        code = error.get(
            "code",
            "UNKNOWN"
        )

        text = error.get(
            "message",
            "Unknown Deriv error"
        )

        set_error(
            f"Deriv {code}: {text}"
        )

        bot["connected"] = False
        bot["connecting"] = False

        return


    msg_type = data.get(
        "msg_type"
    )


    # -----------------------------------------------------
    # AUTHORIZE
    # -----------------------------------------------------

    if msg_type == "authorize":

        auth = data.get(
            "authorize",
            {}
        )

        loginid = auth.get(
            "loginid",
            ""
        )

        currency = auth.get(
            "currency",
            "USD"
        )

        balance = auth.get(
            "balance",
            0
        )


        # Raha nomena Account ID ny user,
        # jereo raha mitovy amin'ilay account authorized.
        expected_account = (
            bot.get("account_id") or ""
        ).strip()


        if (
            expected_account
            and loginid
            and expected_account != loginid
        ):

            set_error(
                "Account ID tsy mifanaraka amin'ilay "
                "kaonty voa-authorize."
            )

            bot["connected"] = False
            bot["connecting"] = False

            try:
                ws.close()
            except Exception:
                pass

            return


        bot["connected"] = True
        bot["connecting"] = False
        bot["balance"] = float(
            balance or 0
        )
        bot["currency"] = currency
        bot["error"] = ""

        set_message(
            f"Connected ✓ Account: {loginid}"
        )


        log(
            f"AUTHORIZE OK: {loginid}"
        )


        # Balance
        request_balance()

        # Chart
        request_chart()

        # Live ticks
        request_tick_stream()


        return


    # -----------------------------------------------------
    # BALANCE
    # -----------------------------------------------------

    if msg_type == "balance":

        balance_data = data.get(
            "balance",
            {}
        )

        try:

            bot["balance"] = float(
                balance_data.get(
                    "balance",
                    0
                )
            )

        except Exception:
            pass


        if balance_data.get(
            "currency"
        ):

            bot["currency"] = (
                balance_data["currency"]
            )


        return


    # -----------------------------------------------------
    # CANDLES
    # -----------------------------------------------------

    if msg_type == "candles":

        candles = data.get(
            "candles",
            []
        )

        process_history_candles(
            candles
        )

        set_message(
            f"Chart live: {bot['symbol']}"
        )

        return


    # -----------------------------------------------------
    # OHLC
    # -----------------------------------------------------

    if msg_type == "ohlc":

        ohlc = data.get(
            "ohlc"
        )

        if not ohlc:
            return


        try:

            candle = {
                "time": int(
                    ohlc["open_time"]
                ),

                "open": float(
                    ohlc["open"]
                ),

                "high": float(
                    ohlc["high"]
                ),

                "low": float(
                    ohlc["low"]
                ),

                "close": float(
                    ohlc["close"]
                )
            }


            chart = bot["chart"]


            if chart:

                if (
                    chart[-1]["time"]
                    == candle["time"]
                ):

                    chart[-1] = candle

                elif (
                    candle["time"]
                    > chart[-1]["time"]
                ):

                    chart.append(
                        candle
                    )

            else:

                chart.append(
                    candle
                )


            bot["chart"] = chart[-300:]

            bot["price"] = candle["close"]

            bot["last_update"] = time.time()


        except Exception as e:

            log(
                f"OHLC error: {e}"
            )


        return


    # -----------------------------------------------------
    # TICK
    # -----------------------------------------------------

    if msg_type == "tick":

        tick = data.get(
            "tick",
            {}
        )

        process_tick(
            tick
        )

        return


# =========================================================
# WEBSOCKET OPEN
# =========================================================

def on_open(ws):

    log(
        "WebSocket CONNECTED."
    )

    bot["connecting"] = True
    bot["connected"] = False

    clear_error()

    set_message(
        "WebSocket connected → authorize..."
    )

    authorize_deriv(
        ws
    )


# =========================================================
# WEBSOCKET ERROR
# =========================================================

def on_error(ws, error):

    log(
        f"WebSocket ERROR: {error}"
    )

    bot["connected"] = False
    bot["connecting"] = False

    set_error(
        f"WebSocket error: {error}"
    )


# =========================================================
# WEBSOCKET CLOSE
# =========================================================

def on_close(
    ws,
    close_status_code,
    close_msg
):

    log(
        f"WebSocket CLOSED: "
        f"{close_status_code} "
        f"{close_msg}"
    )

    bot["connected"] = False
    bot["connecting"] = False
    bot["ws"] = None

    if not bot["error"]:

        set_message(
            "Tapaka ny connexion Deriv."
        )


# =========================================================
# RUN WEBSOCKET
# =========================================================

def websocket_worker():

    app_id = (
        bot["app_id"] or ""
    ).strip()


    if not app_id:

        set_error(
            "Tsy misy App ID."
        )

        return


    # App ID vaovao dia mety ho lava.
    # Aza atao int na mametra ny halavany.
    socket_url = (
        "wss://ws.derivws.com/"
        "websockets/v3"
        f"?app_id={app_id}"
    )


    log(
        "WebSocket URL:"
        " wss://ws.derivws.com/websockets/v3"
        f"?app_id={app_id}"
    )


    try:

        ws = websocket.WebSocketApp(

            socket_url,

            on_open=on_open,

            on_message=on_message,

            on_error=on_error,

            on_close=on_close

        )


        bot["ws"] = ws


        ws.run_forever(
            ping_interval=25,
            ping_timeout=10
        )


    except Exception as e:

        bot["connected"] = False
        bot["connecting"] = False

        set_error(
            f"WebSocket start error: {e}"
        )


    finally:

        bot["ws"] = None


# =========================================================
# CONNECT API
# =========================================================

@app.route(
    "/api/connect",
    methods=["POST"]
)
def connect():

    data = request.get_json(
        silent=True
    ) or {}


    app_id = str(
        data.get(
            "app_id",
            ""
        )
    ).strip()


    pat_token = str(
        data.get(
            "pat_token",
            ""
        )
    ).strip()


    account_id = str(
        data.get(
            "account_id",
            ""
        )
    ).strip()


    symbol = str(
        data.get(
            "symbol",
            "BOOM500"
        )
    ).strip().upper()


    if not app_id:

        return jsonify({
            "ok": False,
            "message":
                "App ID tsy feno."
        }), 400


    if not pat_token:

        return jsonify({
            "ok": False,
            "message":
                "PAT Token tsy feno."
        }), 400


    if not account_id:

        return jsonify({
            "ok": False,
            "message":
                "Account ID tsy feno."
        }), 400


    # Aza aseho amin'ny logs ny token
    log(
        f"CONNECT request:"
        f" app_id={app_id},"
        f" account_id={account_id},"
        f" symbol={symbol}"
    )


    # Close old websocket
    old_ws = bot.get(
        "ws"
    )

    if old_ws:

        try:
            old_ws.close()
        except Exception:
            pass


    # Save configuration
    bot["app_id"] = app_id
    bot["pat_token"] = pat_token
    bot["account_id"] = account_id
    bot["symbol"] = symbol

    bot["connected"] = False
    bot["connecting"] = True
    bot["balance"] = 0.0
    bot["price"] = 0.0
    bot["chart"] = []

    clear_error()

    set_message(
        "Connecting
