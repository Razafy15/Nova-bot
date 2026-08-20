import os
import json
import time
import threading
import requests
import websocket

from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

state = {
    "app_id": "",
    "pat_token": "",
    "account_id": "",

    "connected": False,
    "balance": 0.0,
    "currency": "USD",

    "symbol": "BOOM500",
    "price": 0.0,

    "chart": [],

    "message": "Miandry connexion...",
    "error": ""
}

ws = None
stop_event = threading.Event()


# ==========================================================
# LOG
# ==========================================================

def log(text):
    print(text, flush=True)

    state["message"] = text


# ==========================================================
# DERIV NEW API -> GET OTP
# ==========================================================

def get_otp():

    app_id = state["app_id"].strip()
    pat = state["pat_token"].strip()
    account_id = state["account_id"].strip()

    if not app_id:
        raise Exception("App ID tsy feno.")

    if not pat:
        raise Exception("PAT Token tsy feno.")

    if not account_id:
        raise Exception("Account ID tsy feno.")

    url = (
        "https://api.derivws.com"
        "/trading/v1/options/accounts/"
        + account_id
        + "/otp"
    )

    headers = {
        "Authorization": "Bearer " + pat,
        "Deriv-App-ID": app_id,
        "Content-Type": "application/json"
    }

    log("Maka OTP amin'i Deriv...")

    response = requests.post(
        url,
        headers=headers,
        timeout=20
    )

    print(
        "OTP HTTP:",
        response.status_code,
        flush=True
    )

    try:
        data = response.json()
    except Exception:
        data = {
            "raw": response.text
        }

    if response.status_code not in (200, 201):

        raise Exception(
            "OTP ERROR "
            + str(response.status_code)
            + ": "
            + json.dumps(data)
        )

    # Mety ho data.url na url arakaraka ny response
    url_ws = None

    if isinstance(data, dict):

        if isinstance(
            data.get("data"),
            dict
        ):
            url_ws = data["data"].get("url")

        if not url_ws:
            url_ws = data.get("url")

    if not url_ws:

        raise Exception(
            "Tsy nahazo WebSocket URL avy amin'i Deriv: "
            + json.dumps(data)
        )

    return url_ws


# ==========================================================
# SEND
# ==========================================================

def send(data):

    global ws

    if ws is None:
        return False

    try:

        if ws.sock and ws.sock.connected:

            ws.send(
                json.dumps(data)
            )

            return True

    except Exception as e:

        log(
            "Send error: "
            + str(e)
        )

    return False


# ==========================================================
# BALANCE
# ==========================================================

def request_balance():

    send({
        "balance": 1,
        "subscribe": 1
    })


# ==========================================================
# TICK HISTORY
# ==========================================================

def request_history():

    symbol = state["symbol"]

    send({

        "ticks_history": symbol,

        "count": 500,

        "end": "latest",

        "style": "ticks",

        "subscribe": 1

    })


# ==========================================================
# CREATE CANDLES
# ==========================================================

def make_candles(
    prices,
    times,
    seconds=60
):

    buckets = {}

    for price, timestamp in zip(
        prices,
        times
    ):

        price = float(price)
        timestamp = int(timestamp)

        bucket = (
            timestamp
            - timestamp % seconds
        )

        if bucket not in buckets:

            buckets[bucket] = {
                "time": bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price
            }

        else:

            candle = buckets[bucket]

            candle["high"] = max(
                candle["high"],
                price
            )

            candle["low"] = min(
                candle["low"],
                price
            )

            candle["close"] = price

    return list(
        sorted(
            buckets.values(),
            key=lambda x: x["time"]
        )
    )[-300:]


# ==========================================================
# MESSAGE
# ==========================================================

def on_message(
    socket,
    message
):

    try:

        data = json.loads(
            message
        )

    except Exception as e:

        log(
            "JSON error: "
            + str(e)
        )

        return

    # ------------------------------------------------------
    # ERROR
    # ------------------------------------------------------

    if data.get("error"):

        error = data["error"]

        message_error = error.get(
            "message",
            str(error)
        )

        state["error"] = message_error
        state["connected"] = False

        log(
            "DERIV ERROR: "
            + message_error
        )

        return

    msg_type = data.get(
        "msg_type"
    )

    # ------------------------------------------------------
    # BALANCE
    # ------------------------------------------------------

    if msg_type == "balance":

        balance = data.get(
            "balance",
            {}
        )

        state["balance"] = float(
            balance.get(
                "balance",
                0
            )
        )

        state["currency"] = (
            balance.get(
                "currency",
                "USD"
            )
        )

        log(
            "Balance: "
            + str(
                state["balance"]
            )
            + " "
            + state["currency"]
        )

        return

    # ------------------------------------------------------
    # HISTORY
    # ------------------------------------------------------

    if msg_type == "history":

        history = data.get(
            "history",
            {}
        )

        prices = history.get(
            "prices",
            []
        )

        times = history.get(
            "times",
            []
        )

        if prices and times:

            state["chart"] = (
                make_candles(
                    prices,
                    times
                )
            )

            state["price"] = float(
                prices[-1]
            )

            log(
                "Chart chargé: "
                + str(
                    len(
                        state["chart"]
                    )
                )
                + " candles"
            )

        return

    # ------------------------------------------------------
    # LIVE TICK
    # ------------------------------------------------------

    if msg_type == "tick":

        tick = data.get(
            "tick",
            {}
        )

        price = tick.get(
            "quote"
        )

        epoch = tick.get(
            "epoch"
        )

        if price is None:
            return

        state["price"] = float(
            price
        )

        # Maka chart vaovao amin'ny tick rehetra
        old_chart = state["chart"]

        if not old_chart:

            state["chart"] = [{
                "time": int(epoch),
                "open": float(price),
                "high": float(price),
                "low": float(price),
                "close": float(price)
            }]

            return

        bucket = (
            int(epoch)
            - int(epoch) % 60
        )

        last = old_chart[-1]

        if last["time"] == bucket:

            last["high"] = max(
                last["high"],
                float(price)
            )

            last["low"] = min(
                last["low"],
                float(price)
            )

            last["close"] = float(price)

        else:

            old_chart.append({

                "time": bucket,

                "open": float(price),

                "high": float(price),

                "low": float(price),

                "close": float(price)

            })

            state["chart"] = old_chart[-300:]

        return


# ==========================================================
# OPEN
# ==========================================================

def on_open(socket):

    state["connected"] = True
    state["error"] = ""

    log(
        "WebSocket CONNECTED."
    )

    request_balance()

    time.sleep(0.5)

    request_history()


# ==========================================================
# ERROR
# ==========================================================

def on_error(
    socket,
    error
):

    state["connected"] = False

    state["error"] = str(
        error
    )

    log(
        "WebSocket ERROR: "
        + str(error)
    )


# ==========================================================
# CLOSE
# ==========================================================

def on_close(
    socket,
    code,
    reason
):

    state["connected"] = False

    log(
        "WebSocket CLOSED: "
        + str(code)
        + " "
        + str(reason)
    )


# ==========================================================
# CONNECT WORKER
# ==========================================================

def connection_worker():

    global ws

    try:

        ws_url = get_otp()

        log(
            "OTP OK. Connect WebSocket..."
        )

        ws = websocket.WebSocketApp(

            ws_url,

            on_open=on_open,

            on_message=on_message,

            on_error=on_error,

            on_close=on_close

        )

        ws.run_forever(

            ping_interval=20,

            ping_timeout=10

        )

    except Exception as e:

        state["connected"] = False
        state["error"] = str(e)

        log(
            "CONNECTION FAILED: "
            + str(e)
        )


# ==========================================================
# HOME
# ==========================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


# ==========================================================
# CONNECT
# ==========================================================

@app.route(
    "/api/connect",
    methods=["POST"]
)
def connect():

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

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
    ).strip()

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

    state["app_id"] = app_id
    state["pat_token"] = pat_token
    state["account_id"] = account_id
    state["symbol"] = symbol

    state["error"] = ""

    thread = threading.Thread(
        target=connection_worker,
        daemon=True
    )

    thread.start()

    return jsonify({

        "ok": True,

        "message":
            "Connexion natomboka. "
            "Jereo ny status."

    })


# ==========================================================
# CHANGE SYMBOL
# ==========================================================

@app.route(
    "/api/symbol",
    methods=["POST"]
)
def change_symbol():

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    symbol = str(
        data.get(
            "symbol",
            ""
        )
    ).strip()

    if not symbol:

        return jsonify({
            "ok": False
        }), 400

    state["symbol"] = symbol

    if state["connected"]:

        request_history()

    return jsonify({
        "ok": True
    })


# ==========================================================
# STATS
# ==========================================================

@app.route(
    "/api/stats",
    methods=["GET"]
)
def stats():

    return jsonify({

        "connected":
            state["connected"],

        "balance":
            state["balance"],

        "currency":
            state["currency"],

        "symbol":
            state["symbol"],

        "price":
            state["price"],

        "chart":
            state["chart"],

        "message":
            state["message"],

        "error":
            state["error"]

    })


# ==========================================================
# HEALTH
# ==========================================================

@app.route("/health")
def health():

    return jsonify({
        "status": "ok"
    })


# ==========================================================
# RUN
# ==========================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            10000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
