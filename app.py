import os
import json
import time
import threading

import requests
import websocket

from flask import Flask, render_template, request, jsonify


app = Flask(__name__)


# =========================================================
# STATE
# =========================================================

state = {
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


# =========================================================
# LOGGING
# =========================================================

def log(message):
    print(
        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}",
        flush=True
    )


def set_message(message):
    state["message"] = str(message)
    log(str(message))


def set_error(message):
    state["error"] = str(message)
    state["message"] = str(message)

    log("ERROR: " + str(message))


def clear_error():
    state["error"] = ""


# =========================================================
# DERIV REST - GET OTP
# =========================================================

def get_authenticated_ws_url(
    app_id,
    pat_token,
    account_id
):

    url = (
        "https://api.derivws.com"
        f"/trading/v1/options/accounts/"
        f"{account_id}/otp"
    )

    headers = {
        "Authorization": f"Bearer {pat_token}",
        "Deriv-App-ID": app_id,
        "Content-Type": "application/json",
    }

    log("Mangataka OTP amin'i Deriv...")

    response = requests.post(
        url,
        headers=headers,
        timeout=20,
    )

    log(
        f"Deriv OTP HTTP status: "
        f"{response.status_code}"
    )

    try:
        data = response.json()
    except Exception:
        raise RuntimeError(
            "Deriv namerina réponse tsy JSON."
        )

    if response.status_code != 200:

        errors = data.get("errors", [])

        if errors:

            first = errors[0]

            code = first.get(
                "code",
                "UNKNOWN"
            )

            message = first.get(
                "message",
                "Unknown Deriv error"
            )

            raise RuntimeError(
                f"Deriv {code}: {message}"
            )

        raise RuntimeError(
            f"Deriv HTTP {response.status_code}: "
            f"{data}"
        )

    payload = data.get(
        "data",
        {}
    )

    ws_url = payload.get(
        "url"
    )

    if not ws_url:

        raise RuntimeError(
            "OTP OK fa tsy nahazo WebSocket URL "
            "avy amin'i Deriv."
        )

    return ws_url


# =========================================================
# WEBSOCKET SEND
# =========================================================

def ws_send(data):

    ws = state.get("ws")

    if ws is None:
        return False

    try:

        text = json.dumps(data)

        with ws_lock:
            ws.send(text)

        return True

    except Exception as e:

        set_error(
            f"WebSocket send error: {e}"
        )

        return False


# =========================================================
# REQUEST BALANCE
# =========================================================

def request_balance():

    return ws_send({
        "balance": 1,
        "subscribe": 1,
        "req_id": 100
    })


# =========================================================
# REQUEST CANDLES
# =========================================================

def request_chart():

    symbol = state["symbol"]

    log(
        f"Mangataka candles: {symbol}"
    )

    return ws_send({
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": 300,
        "end": "latest",
        "style": "candles",
        "granularity": 60,
        "req_id": 200
    })


# =========================================================
# REQUEST LIVE TICKS
# =========================================================

def request_ticks():

    symbol = state["symbol"]

    log(
        f"Mangataka live ticks: {symbol}"
    )

    return ws_send({
        "ticks": symbol,
        "subscribe": 1,
        "req_id": 300
    })


# =========================================================
# PROCESS CANDLES
# =========================================================

def process_candles(candles):

    result = []

    for item in candles:

        try:

            result.append({
                "time": int(
                    item["epoch"]
                ),

                "open": float(
                    item["open"]
                ),

                "high": float(
                    item["high"]
                ),

                "low": float(
                    item["low"]
                ),

                "close": float(
                    item["close"]
                )
            })

        except Exception as e:

            log(
                f"Candle invalid: {e}"
            )

    result.sort(
        key=lambda x: x["time"]
    )

    # Remove duplicate timestamps
    unique = []

    seen = set()

    for candle in result:

        timestamp = candle["time"]

        if timestamp in seen:
            continue

        seen.add(timestamp)

        unique.append(candle)

    state["chart"] = unique[-300:]

    if state["chart"]:

        state["price"] = (
            state["chart"][-1]["close"]
        )

        state["last_update"] = time.time()


# =========================================================
# PROCESS TICK
# =========================================================

def process_tick(tick):

    try:

        quote = float(
            tick["quote"]
        )

        state["price"] = quote

        state["last_update"] = (
            time.time()
        )

    except Exception:
        pass


# =========================================================
# WEBSOCKET MESSAGE
# =========================================================

def on_message(
    ws,
    message
):

    try:

        data = json.loads(
            message
        )

    except Exception as e:

        log(
            f"JSON error: {e}"
        )

        return

    # -----------------------------------------------------
    # ERROR
    # -----------------------------------------------------

    if "error" in data:

        error = data.get(
            "error",
            {}
        )

        code = error.get(
            "code",
            "UNKNOWN"
        )

        message_text = error.get(
            "message",
            "Unknown error"
        )

        set_error(
            f"Deriv {code}: "
            f"{message_text}"
        )

        return

    msg_type = data.get(
        "msg_type"
    )

    # -----------------------------------------------------
    # BALANCE
    # -----------------------------------------------------

    if msg_type == "balance":

        balance_data = data.get(
            "balance",
            {}
        )

        try:

            state["balance"] = float(
                balance_data.get(
                    "balance",
                    0
                )
            )

        except Exception:
            pass

        currency = (
            balance_data.get(
                "currency"
            )
        )

        if currency:
            state["currency"] = currency

        state["last_update"] = (
            time.time()
        )

        if not state["error"]:

            set_message(
                "Connexion + Balance OK ✓"
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

        process_candles(
            candles
        )

        if not state["error"]:

            set_message(
                f"Chart live: "
                f"{state['symbol']}"
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
        "Authenticated WebSocket OPEN ✓"
    )

    state["connected"] = True
    state["connecting"] = False

    clear_error()

    set_message(
        "Deriv connecté ✓"
    )

    # Account data
    request_balance()

    # Chart
    request_chart()

    # Live price
    request_ticks()


# =========================================================
# WEBSOCKET ERROR
# =========================================================

def on_error(
    ws,
    error
):

    log(
        f"WebSocket ERROR: {error}"
    )

    state["connected"] = False
    state["connecting"] = False

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

    state["connected"] = False
    state["connecting"] = False
    state["ws"] = None

    if not state["error"]:

        set_message(
            "Tapaka ny connexion Deriv."
        )


# =========================================================
# CONNECT THREAD
# =========================================================

def connect_worker():

    app_id = state["app_id"]
    pat_token = state["pat_token"]
    account_id = state["account_id"]

    try:

        state["connecting"] = True
        state["connected"] = False

        clear_error()

        set_message(
            "Mangataka authentication..."
        )

        # -------------------------------------------------
        # 1. REST OTP
        # -------------------------------------------------

        ws_url = get_authenticated_ws_url(
            app_id,
            pat_token,
            account_id
        )

        log(
            "OTP nahomby ✓"
        )

        # Aza atao log ny URL satria misy OTP ao anatiny.

        set_message(
            "OTP OK → mampifandray WebSocket..."
        )

        # -------------------------------------------------
        # 2. AUTHENTICATED WEBSOCKET
        # -------------------------------------------------

        ws = websocket.WebSocketApp(

            ws_url,

            on_open=on_open,

            on_message=on_message,

            on_error=on_error,

            on_close=on_close
        )

        state["ws"] = ws

        ws.run_forever(
            ping_interval=20,
            ping_timeout=10
        )

    except Exception as e:

        state["connected"] = False
        state["connecting"] = False

        set_error(
            str(e)
        )

    finally:

        state["ws"] = None


# =========================================================
# CONNECT API
# =========================================================

@app.route(
    "/api/connect",
    methods=["POST"]
)
def api_connect():

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

    # -----------------------------------------------------
    # VALIDATION
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # CLOSE OLD CONNECTION
    # -----------------------------------------------------

    old_ws = state.get(
        "ws"
    )

    if old_ws:

        try:
            old_ws.close()
        except Exception:
            pass

    # -----------------------------------------------------
    # SAVE CONFIG
    # -----------------------------------------------------

    state["app_id"] = app_id
    state["pat_token"] = pat_token
    state["account_id"] = account_id
    state["symbol"] = symbol

    state["connected"] = False
    state["connecting"] = True

    state["balance"] = 0.0
    state["price"] = 0.0

    state["chart"] = []

    clear_error()

    set_message(
        "Connexion amin'i Deriv..."
    )

    # -----------------------------------------------------
    # START THREAD
    # -----------------------------------------------------

    thread = threading.Thread(
        target=connect_worker,
        daemon=True
    )

    thread.start()

    return jsonify({
        "ok": True,
        "message":
            "Connexion natomboka."
    })


# =========================================================
# CHANGE SYMBOL
# =========================================================

@app.route(
    "/api/symbol",
    methods=["POST"]
)
def api_symbol():

    data = request.get_json(
        silent=True
    ) or {}

    symbol = str(
        data.get(
            "symbol",
            "BOOM500"
        )
    ).strip().upper()

    state["symbol"] = symbol

    state["chart"] = []
    state["price"] = 0.0

    if state["connected"]:

        request_chart()
        request_ticks()

    set_message(
        f"Market: {symbol}"
    )

    return jsonify({
        "ok": True,
        "symbol": symbol
    })


# =========================================================
# STATS
# =========================================================

@app.route(
    "/api/stats",
    methods=["GET"]
)
def api_stats():

    return jsonify({

        "connected":
            bool(
                state["connected"]
            ),

        "connecting":
            bool(
                state["connecting"]
            ),

        "balance":
            state["balance"],

        "currency":
            state["currency"],

        "price":
            state["price"],

        "symbol":
            state["symbol"],

        "chart":
            state["chart"][-300:],

        "message":
            state["message"],

        "error":
            state["error"],

        "last_update":
            state["last_update"]
    })


# =========================================================
# HOME
# =========================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


# =========================================================
# HEALTH
# =========================================================

@app.route(
    "/health"
)
def health():

    return jsonify({
        "status": "ok",
        "service": "Nova-bot"
    })


# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    port = int(
        os.environ.get(
            "PORT",
            5000
        )
    )

    app.run(
        host="0.0.0.0",
        port=port
    )
