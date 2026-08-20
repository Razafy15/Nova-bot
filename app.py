import os
import json
import time
import threading
import requests
import websocket

from flask import Flask, render_template, request, jsonify

app = Flask(__name__)

# =========================
# CONFIG
# =========================

state = {
    "connected": False,
    "trading_connected": False,

    "app_id": "",
    "account_id": "",
    "account_type": "demo",

    "symbol": "BOOM1000",
    "timeframe": 60,

    "balance": 0.0,
    "currency": "USD",

    "price": 0.0,
    "candles": [],

    "bot_running": False,

    "pre_signal": "WAIT",
    "confirmed_signal": "WAIT",
    "confidence": 0,

    "message": "Miandry connection...",

    "scanner": {},

    "api_token_set": False
}

# Public market-data socket
public_ws = None
public_lock = threading.Lock()

# Authenticated trading socket
trade_ws = None
trade_lock = threading.Lock()

# Current chart subscription
chart_symbol = None


# =========================
# MARKET LIST
# =========================

BOOM_CRASH = [
    "BOOM300",
    "BOOM500",
    "BOOM600",
    "BOOM900",
    "BOOM1000",
    "BOOM1500",
    "BOOM300N",
    "BOOM500N",
    "BOOM600N",
    "BOOM900N",
    "BOOM1000N",
    "BOOM1500N",

    "CRASH300",
    "CRASH500",
    "CRASH600",
    "CRASH900",
    "CRASH1000",
    "CRASH1500",
    "CRASH300N",
    "CRASH500N",
    "CRASH600N",
    "CRASH900N",
    "CRASH1000N",
    "CRASH1500N",
]

VOLATILITY = [
    "1HZ10V",
    "1HZ25V",
    "1HZ50V",
    "1HZ75V",
    "1HZ100V",
]


# =========================
# HELPERS
# =========================

def send_public(data):
    global public_ws

    with public_lock:
        if public_ws and public_ws.sock and public_ws.sock.connected:
            try:
                public_ws.send(json.dumps(data))
                return True
            except Exception as e:
                print("Public WS send error:", e)

    return False


def send_trade(data):
    global trade_ws

    with trade_lock:
        if trade_ws and trade_ws.sock and trade_ws.sock.connected:
            try:
                trade_ws.send(json.dumps(data))
                return True
            except Exception as e:
                print("Trade WS send error:", e)

    return False


# =========================
# PUBLIC WS
# =========================

def public_on_open(ws):
    print("PUBLIC WS CONNECTED")

    state["connected"] = True
    state["message"] = "Market data connected"

    subscribe_chart(state["symbol"])


def public_on_message(ws, message):
    global chart_symbol

    try:
        data = json.loads(message)
    except Exception:
        return

    msg_type = data.get("msg_type")

    # ---------------------
    # Historical candles
    # ---------------------

    if msg_type == "candles":

        candles = data.get("candles", [])

        converted = []

        for c in candles:
            try:
                converted.append({
                    "time": int(c["epoch"]),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"])
                })
            except Exception:
                pass

        state["candles"] = converted[-300:]

        if converted:
            state["price"] = converted[-1]["close"]

        calculate_signal()


    # ---------------------
    # Live OHLC
    # ---------------------

    elif msg_type == "ohlc":

        ohlc = data.get("ohlc")

        if not ohlc:
            return

        try:
            candle = {
                "time": int(ohlc["open_time"]),
                "open": float(ohlc["open"]),
                "high": float(ohlc["high"]),
                "low": float(ohlc["low"]),
                "close": float(ohlc["close"])
            }
        except Exception:
            return

        candles = state["candles"]

        if candles and candles[-1]["time"] == candle["time"]:
            candles[-1] = candle
        else:
            candles.append(candle)

        state["candles"] = candles[-300:]
        state["price"] = candle["close"]

        calculate_signal()


    # ---------------------
    # Tick
    # ---------------------

    elif msg_type == "tick":

        tick = data.get("tick")

        if tick:
            try:
                state["price"] = float(tick["quote"])
            except Exception:
                pass


    elif msg_type == "error":

        error = data.get("error", {})
        state["message"] = error.get(
            "message",
            "Deriv API error"
        )


def public_on_error(ws, error):
    print("PUBLIC WS ERROR:", error)
    state["connected"] = False
    state["message"] = "Market data connection error"


def public_on_close(ws, code, msg):
    print("PUBLIC WS CLOSED:", code, msg)

    state["connected"] = False

    # Reconnect after a short delay
    time.sleep(3)

    start_public_ws()


def start_public_ws():
    global public_ws

    try:

        url = "wss://api.derivws.com/trading/v1/options/ws/public"

        public_ws = websocket.WebSocketApp(
            url,
            on_open=public_on_open,
            on_message=public_on_message,
            on_error=public_on_error,
            on_close=public_on_close
        )

        thread = threading.Thread(
            target=public_ws.run_forever,
            daemon=True
        )

        thread.start()

    except Exception as e:

        print("Cannot start public WS:", e)


# =========================
# CHART
# =========================

def subscribe_chart(symbol):

    global chart_symbol

    if not symbol:
        return

    chart_symbol = symbol

    # Historical candles
    send_public({
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": 300,
        "end": "latest",
        "style": "candles",
        "granularity": state["timeframe"],
        "subscribe": 1
    })


# =========================
# SIGNAL ENGINE
# =========================

def calculate_signal():

    candles = state["candles"]

    if len(candles) < 20:
        state["pre_signal"] = "WAIT"
        state["confirmed_signal"] = "WAIT"
        state["confidence"] = 0
        return

    closes = [
        float(c["close"])
        for c in candles
    ]

    # Simple momentum used only for dashboard testing.
    # The real SMC/Spike strategy will be added later.

    short = sum(closes[-5:]) / 5
    long = sum(closes[-20:]) / 20

    symbol = state["symbol"].upper()

    if short > long:
        signal = "CALL"
        confidence = min(
            95,
            int(60 + abs(short - long) / max(long, 0.00001) * 10000)
        )
    elif short < long:
        signal = "PUT"
        confidence = min(
            95,
            int(60 + abs(short - long) / max(long, 0.00001) * 10000)
        )
    else:
        signal = "WAIT"
        confidence = 0

    state["pre_signal"] = signal
    state["confidence"] = confidence

    # Confirmation is intentionally conservative for now.
    if confidence >= 75:
        state["confirmed_signal"] = signal
    else:
        state["confirmed_signal"] = "WAIT"


# =========================
# DERIV AUTH
# =========================

def get_otp_ws_url():

    app_id = state["app_id"]
    token = state.get("_api_token")

    account_id = state["account_id"]

    if not app_id:
        raise Exception("App ID tsy feno")

    if not token:
        raise Exception("API Token tsy feno")

    if not account_id:
        raise Exception("Account ID tsy feno")

    url = (
        "https://api.derivws.com"
        f"/trading/v1/options/accounts/{account_id}/otp"
    )

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    response = requests.post(
        url,
        headers=headers,
        timeout=20
    )

    if response.status_code != 200:
        try:
            body = response.json()
        except Exception:
            body = response.text

        raise Exception(
            f"OTP error {response.status_code}: {body}"
        )

    data = response.json()

    ws_url = data.get("data", {}).get("url")

    if not ws_url:
        raise Exception("Tsy nahazo WebSocket URL")

    return ws_url


def trade_on_open(ws):

    print("TRADING WS CONNECTED")

    state["trading_connected"] = True
    state["message"] = "Deriv account connected"

    # Balance
    send_trade({
        "balance": 1,
        "subscribe": 1,
        "req_id": 1001
    })


def trade_on_message(ws, message):

    try:
        data = json.loads(message)
    except Exception:
        return

    msg_type = data.get("msg_type")

    if msg_type == "balance":

        balance = data.get("balance", {})

        try:
            state["balance"] = float(
                balance.get("balance", 0)
            )
        except Exception:
            pass

        state["currency"] = balance.get(
            "currency",
            "USD"
        )


    elif msg_type == "error":

        error = data.get("error", {})

        state["message"] = error.get(
            "message",
            "Trading API error"
        )


def trade_on_error(ws, error):

    print("TRADING WS ERROR:", error)

    state["trading_connected"] = False
    state["message"] = "Trading connection error"


def trade_on_close(ws, code, msg):

    print("TRADING WS CLOSED")

    state["trading_connected"] = False


def connect_trading():

    global trade_ws

    try:

        ws_url = get_otp_ws_url()

        trade_ws = websocket.WebSocketApp(
            ws_url,
            on_open=trade_on_open,
            on_message=trade_on_message,
            on_error=trade_on_error,
            on_close=trade_on_close
        )

        thread = threading.Thread(
            target=trade_ws.run_forever,
            daemon=True
        )

        thread.start()

    except Exception as e:

        print("Trading connection error:", e)

        state["trading_connected"] = False
        state["message"] = str(e)


# =========================
# MARKET SCANNER
# =========================

def scanner_worker():

    while True:

        try:

            scanner = {}

            for symbol in BOOM_CRASH + VOLATILITY:

                scanner[symbol] = {
                    "symbol": symbol,
                    "type": (
                        "BOOM/CRASH"
                        if symbol.startswith(("BOOM", "CRASH"))
                        else "VOLATILITY"
                    ),
                    "signal": "WAIT",
                    "confidence": 0
                }

            state["scanner"] = scanner

        except Exception as e:

            print("Scanner error:", e)

        time.sleep(5)


# =========================
# ROUTES
# =========================

@app.route("/")
def index():

    return render_template(
        "index.html",
        markets=BOOM_CRASH + VOLATILITY
    )


@app.route("/api/connect", methods=["POST"])
def api_connect():

    data = request.get_json() or {}

    app_id = str(
        data.get("app_id", "")
    ).strip()

    token = str(
        data.get("api_token", "")
    ).strip()

    account_id = str(
        data.get("account_id", "")
    ).strip()

    account_type = str(
        data.get("account_type", "demo")
    ).lower()

    if not app_id:
        return jsonify({
            "ok": False,
            "message": "Ampidiro ny App ID."
        }), 400

    if not token:
        return jsonify({
            "ok": False,
            "message": "Ampidiro ny API Token."
        }), 400

    if not account_id:
        return jsonify({
            "ok": False,
            "message": "Ampidiro ny Account ID."
        }), 400

    state["app_id"] = app_id
    state["account_id"] = account_id
    state["account_type"] = account_type

    # Stored only in server memory.
    # Never written to GitHub.
    state["_api_token"] = token
    state["api_token_set"] = True

    threading.Thread(
        target=connect_trading,
        daemon=True
    ).start()

    return jsonify({
        "ok": True,
        "message": "Connection started..."
    })


@app.route("/api/disconnect", methods=["POST"])
def api_disconnect():

    global trade_ws

    try:

        if trade_ws:
            trade_ws.close()

    except Exception:
        pass

    state["trading_connected"] = False
    state["api_token_set"] = False

    state.pop("_api_token", None)

    return jsonify({
        "ok": True,
        "message": "Disconnected"
    })


@app.route("/api/chart", methods=["POST"])
def api_chart():

    data = request.get_json() or {}

    symbol = str(
        data.get("symbol", "")
    ).upper()

    timeframe = int(
        data.get("timeframe", 60)
    )

    if not symbol:
        return jsonify({
            "ok": False,
            "message": "Symbol tsy feno"
        }), 400

    allowed_timeframes = [
        60,
        120,
        180,
        300,
        600,
        900,
        1800
    ]

    if timeframe not in allowed_timeframes:
        timeframe = 60

    state["symbol"] = symbol
    state["timeframe"] = timeframe
    state["candles"] = []

    subscribe_chart(symbol)

    return jsonify({
        "ok": True,
        "symbol": symbol,
        "timeframe": timeframe
    })


@app.route("/api/stats")
def api_stats():

    public_state = {
        key: value
        for key, value in state.items()
        if key != "_api_token"
    }

    return jsonify(public_state)


@app.route("/api/start", methods=["POST"])
def api_start():

    # IMPORTANT:
    # This version does not place real trades yet.
    # It only activates the strategy/signal engine.

    state["bot_running"] = True
    state["message"] = (
        "Bot running — signal mode. "
        "Auto-trading mbola OFF."
    )

    return jsonify({
        "ok": True,
        "message": "Bot started in signal mode."
    })


@app.route("/api/stop", methods=["POST"])
def api_stop():

    state["bot_running"] = False
    state["message"] = "Bot stopped."

    return jsonify({
        "ok": True,
        "message": "Bot stopped."
    })


# =========================
# START
# =========================

if __name__ == "__main__":

    threading.Thread(
        target=start_public_ws,
        daemon=True
    ).start()

    threading.Thread(
        target=scanner_worker,
        daemon=True
    ).start()

    port = int(
        os.environ.get("PORT", 5000)
    )

    app.run(
        host="0.0.0.0",
        port=port,
        threaded=True
    )
