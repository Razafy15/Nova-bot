import os
import json
import time
import threading

import requests
import websocket

from flask import Flask, render_template, request, jsonify


app = Flask(__name__)


# =========================================================
# CONFIG
# =========================================================

API_BASE = "https://api.derivws.com"

DEFAULT_SYMBOL = "BOOM500"


# =========================================================
# BOT STATE
# =========================================================

bot = {
    "app_id": "",
    "pat_token": "",
    "account_id": "",
    "symbol": DEFAULT_SYMBOL,

    "connected": False,
    "connecting": False,

    "balance": 0.0,
    "currency": "USD",

    "price": 0.0,

    "candles": [],

    "signal": "WAIT",
    "signal_text": "Miandry market data...",
    "signal_strength": 0,

    "message": "Vonona hifandray amin'i Deriv.",
    "error": "",

    "last_update": 0,

    "ws": None,
    "ws_url": "",

    "connection_started": 0,
}


ws_lock = threading.Lock()


# =========================================================
# BOOM / CRASH SYMBOLS
# =========================================================

BOOM_CRASH_SYMBOLS = [
    "BOOM300",
    "BOOM500",
    "BOOM600",
    "BOOM900",
    "BOOM1000",

    "CRASH300",
    "CRASH500",
    "CRASH600",
    "CRASH900",
    "CRASH1000",
]


# Volatility = signal only
VOLATILITY_SYMBOLS = [
    "1HZ10V",
    "1HZ15V",
    "1HZ25V",
    "1HZ30V",
    "1HZ50V",
    "1HZ75V",
    "1HZ90V",
    "1HZ100V",

    "R_10",
    "R_25",
    "R_50",
    "R_75",
    "R_100",
]


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
    log(str(message))


def set_error(message):
    bot["error"] = str(message)
    bot["message"] = str(message)

    log("ERROR: " + str(message))


def clear_error():
    bot["error"] = ""


def reset_market_data():

    bot["price"] = 0.0
    bot["candles"] = []

    bot["signal"] = "WAIT"
    bot["signal_text"] = "Miandry market data..."
    bot["signal_strength"] = 0


# =========================================================
# REST - GET OTP
# =========================================================

def get_websocket_url():

    app_id = bot["app_id"]
    token = bot["pat_token"]
    account_id = bot["account_id"]

    if not app_id:
        raise Exception("App ID tsy feno.")

    if not token:
        raise Exception("PAT Token tsy feno.")

    if not account_id:
        raise Exception("Account ID tsy feno.")


    url = (
        f"{API_BASE}/trading/v1/options/"
        f"accounts/{account_id}/otp"
    )


    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


    log(
        "Mangataka WebSocket OTP amin'i Deriv..."
    )


    response = requests.post(
        url,
        headers=headers,
        timeout=20
    )


    log(
        f"OTP HTTP status: {response.status_code}"
    )


    try:
        data = response.json()
    except Exception:
        raise Exception(
            f"Deriv namaly zavatra tsy JSON: "
            f"{response.text[:300]}"
        )


    if response.status_code != 200:

        errors = data.get(
            "errors",
            []
        )

        if errors:

            err = errors[0]

            code = err.get(
                "code",
                "UNKNOWN"
            )

            message = err.get(
                "message",
                "Unknown error"
            )

            raise Exception(
                f"{code}: {message}"
            )

        raise Exception(
            f"HTTP {response.status_code}: "
            f"{data}"
        )


    result = data.get(
        "data",
        {}
    )


    ws_url = result.get(
        "url"
    )


    if not ws_url:
        raise Exception(
            "Tsy nahazo WebSocket URL avy amin'i Deriv."
        )


    return ws_url


# =========================================================
# WEBSOCKET SEND
# =========================================================

def send_ws(payload):

    ws = bot.get("ws")

    if ws is None:
        return False


    try:

        with ws_lock:

            ws.send(
                json.dumps(payload)
            )

        return True


    except Exception as e:

        log(
            f"WS send error: {e}"
        )

        return False


# =========================================================
# REQUEST BALANCE
# =========================================================

def request_balance():

    send_ws({
        "balance": 1,
        "subscribe": 1,
        "req_id": 100
    })


# =========================================================
# REQUEST CANDLES
# =========================================================

def request_candles():

    symbol = bot["symbol"]

    log(
        f"Mangataka candles: {symbol}"
    )


    send_ws({

        "ticks_history": symbol,

        "adjust_start_time": 1,

        "count": 300,

        "end": "latest",

        "style": "candles",

        "granularity": 60,

        "subscribe": 1,

        "req_id": 200

    })


# =========================================================
# SIGNAL ENGINE
# =========================================================

def calculate_signal():

    candles = bot["candles"]

    if len(candles) < 20:

        bot["signal"] = "WAIT"

        bot["signal_text"] = (
            "Miandry candles..."
        )

        bot["signal_strength"] = 0

        return


    symbol = bot["symbol"].upper()


    closes = [
        float(c["close"])
        for c in candles
    ]


    # -----------------------------------------------------
    # Simple EMA calculation
    # -----------------------------------------------------

    def ema(values, period):

        if len(values) < period:
            return values[-1]

        alpha = 2 / (
            period + 1
        )

        value = values[0]

        for price in values[1:]:

            value = (
                alpha * price
                + (1 - alpha) * value
            )

        return value


    ema9 = ema(
        closes[-60:],
        9
    )

    ema21 = ema(
        closes[-60:],
        21
    )


    # -----------------------------------------------------
    # Volatility = SIGNAL ONLY
    # -----------------------------------------------------

    if symbol in VOLATILITY_SYMBOLS:

        if ema9 > ema21:

            bot["signal"] = "CALL"

            bot["signal_text"] = (
                f"Volatility signal: "
                f"UP | EMA9 > EMA21"
            )

            bot["signal_strength"] = 70

        elif ema9 < ema21:

            bot["signal"] = "PUT"

            bot["signal_text"] = (
                f"Volatility signal: "
                f"DOWN | EMA9 < EMA21"
            )

            bot["signal_strength"] = 70

        else:

            bot["signal"] = "WAIT"

            bot["signal_text"] = (
                "Volatility: WAIT"
            )

            bot["signal_strength"] = 0

        return


    # -----------------------------------------------------
    # BOOM
    # -----------------------------------------------------

    if symbol.startswith("BOOM"):

        recent = candles[-5:]

        green = 0

        for c in recent:

            if c["close"] > c["open"]:
                green += 1


        if (
            green >= 3
            and ema9 > ema21
        ):

            bot["signal"] = "PUT"

            bot["signal_text"] = (
                "BOOM signal: "
                "possible spike detected"
            )

            bot["signal_strength"] = 80

        else:

            bot["signal"] = "WAIT"

            bot["signal_text"] = (
                "BOOM: miandry spike..."
            )

            bot["signal_strength"] = 0

        return


    # -----------------------------------------------------
    # CRASH
    # -----------------------------------------------------

    if symbol.startswith("CRASH"):

        recent = candles[-5:]

        red = 0

        for c in recent:

            if c["close"] < c["open"]:
                red += 1


        if (
            red >= 3
            and ema9 < ema21
        ):

            bot["signal"] = "CALL"

            bot["signal_text"] = (
                "CRASH signal: "
                "possible spike detected"
            )

            bot["signal_strength"] = 80

        else:

            bot["signal"] = "WAIT"

            bot["signal_text"] = (
                "CRASH: miandry spike..."
            )

            bot["signal_strength"] = 0

        return


    bot["signal"] = "WAIT"

    bot["signal_text"] = (
        "Market tsy voafaritra."
    )

    bot["signal_strength"] = 0


# =========================================================
# FORMAT CANDLES
# =========================================================

def set_candles(candles):

    result = []

    seen = set()


    for candle in candles:

        try:

            epoch = int(
                candle["epoch"]
            )

            if epoch in seen:
                continue

            seen.add(epoch)


            result.append({

                "time": epoch,

                "open": float(
                    candle["open"]
                ),

                "high": float(
                    candle["high"]
                ),

                "low": float(
                    candle["low"]
                ),

                "close": float(
                    candle["close"]
                )

            })

        except Exception as e:

            log(
                f"Candle error: {e}"
            )


    result.sort(
        key=lambda x: x["time"]
    )


    bot["candles"] = result[-300:]


    if bot["candles"]:

        bot["price"] = (
            bot["candles"][-1]["close"]
        )

        bot["last_update"] = time.time()


    calculate_signal()


# =========================================================
# UPDATE LIVE OHLC
# =========================================================

def update_ohlc(ohlc):

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


        candles = bot["candles"]


        if not candles:

            candles.append(
                candle
            )

        elif (
            candles[-1]["time"]
            == candle["time"]
        ):

            candles[-1] = candle

        elif (
            candle["time"]
            > candles[-1]["time"]
        ):

            candles.append(
                candle
            )


        bot["candles"] = candles[-300:]

        bot["price"] = candle["close"]

        bot["last_update"] = time.time()


        calculate_signal()


    except Exception as e:

        log(
            f"OHLC update error: {e}"
        )


# =========================================================
# WEBSOCKET MESSAGE
# =========================================================

def on_message(ws, message):

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
    # DERIV ERROR
    # -----------------------------------------------------

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
            f"{code}: {text}"
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


        balance = auth.get(
            "balance",
            0
        )


        currency = auth.get(
            "currency",
            "USD"
        )


        # Account verification
        if (
            bot["account_id"]
            and loginid
            and bot["account_id"]
            != loginid
        ):

            set_error(
                "Account ID tsy mifanaraka "
                f"({loginid})."
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

        clear_error()


        set_message(
            f"Connected ✓ {loginid}"
        )


        log(
            f"AUTHORIZED: {loginid}"
        )


        request_balance()

        request_candles()

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


        currency = (
            balance_data.get(
                "currency"
            )
        )


        if currency:

            bot["currency"] = currency


        return


    # -----------------------------------------------------
    # HISTORICAL CANDLES
    # -----------------------------------------------------

    if msg_type == "candles":

        candles = data.get(
            "candles",
            []
        )


        set_candles(
            candles
        )


        set_message(
            f"Market live • {bot['symbol']}"
        )


        return


    # -----------------------------------------------------
    # LIVE OHLC
    # -----------------------------------------------------

    if msg_type == "ohlc":

        ohlc = data.get(
            "ohlc"
        )


        if ohlc:

            update_ohlc(
                ohlc
            )


        return


    # -----------------------------------------------------
    # PING / OTHER
    # -----------------------------------------------------

    if msg_type == "ping":

        return


# =========================================================
# WEBSOCKET OPEN
# =========================================================

def on_open(ws):

    log(
        "Authenticated WebSocket OPEN."
    )


    bot["connecting"] = True
    bot["connected"] = False


    clear_error()


    set_message(
        "WebSocket connected • loading account..."
    )


# =========================================================
# WEBSOCKET ERROR
# =========================================================

def on_error(ws, error):

    log(
        f"WebSocket error: {error}"
    )


    bot["connected"] = False
    bot["connecting"] = False


    set_error(
        f"WebSocket: {error}"
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
        f"WebSocket closed: "
        f"{close_status_code} "
        f"{close_msg}"
    )


    bot["connected"] = False
    bot["connecting"] = False

    bot["ws"] = None


    if not bot["error"]:

        set_message(
            "Tapaka ny connexion."
        )


# =========================================================
# CONNECT WORKER
# =========================================================

def connect_worker():

    try:

        # Step 1:
        # REST OTP
        ws_url = get_websocket_url()


        bot["ws_url"] = ws_url


        # Step 2:
        # connect immediately
        # OTP is short lived
        ws = websocket.WebSocketApp(

            ws_url,

            on_open=on_open,

            on_message=on_message,

            on_error=on_error,

            on_close=on_close

        )


        bot["ws"] = ws


        log(
            "Opening authenticated WebSocket..."
        )


        ws.run_forever(
            ping_interval=25,
            ping_timeout=10
        )


    except Exception as e:

        bot["connected"] = False
        bot["connecting"] = False

        set_error(
            str(e)
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
            DEFAULT_SYMBOL
        )
    ).strip().upper()


    if not app_id:

        return jsonify({
            "ok": False,
            "message": "App ID tsy feno."
        }), 400


    if not pat_token:

        return jsonify({
            "ok": False,
            "message": "PAT Token tsy feno."
        }), 400


    if not account_id:

        return jsonify({
            "ok": False,
            "message":
                "Account ID tsy feno."
        }), 400


    # Close old connection
    old_ws = bot.get(
        "ws"
    )


    if old_ws:

        try:
            old_ws.close()
        except Exception:
            pass


    bot["app_id"] = app_id
    bot["pat_token"] = pat_token
    bot["account_id"] = account_id
    bot["symbol"] = symbol

    bot["connected"] = False
    bot["connecting"] = True

    bot["connection_started"] = time.time()

    bot["message"] = (
        "Connecting amin'i Deriv..."
    )

    bot["error"] = ""

    reset_market_data()


    log(
        "New connection requested"
    )

    log(
        f"App ID: {app_id}"
    )

    log(
        f"Account ID: {account_id}"
    )

    log(
        f"Symbol: {symbol}"
    )

    # Never print PAT token


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
            DEFAULT_SYMBOL
        )
    ).strip().upper()


    bot["symbol"] = symbol


    reset_market_data()


    if bot["connected"]:

        request_candles()


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
                bot["connected"]
            ),

        "connecting":
            bool(
                bot["connecting"]
            ),

        "balance":
            bot["balance"],

        "currency":
            bot["currency"],

        "price":
            bot["price"],

        "symbol":
            bot["symbol"],

        "candles":
            bot["candles"][-300:],

        "signal":
            bot["signal"],

        "signal_text":
            bot["signal_text"],

        "signal_strength":
            bot["signal_strength"],

        "message":
            bot["message"],

        "error":
            bot["error"],

        "last_update":
            bot["last_update"]

    })


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
# HOME
# =========================================================

@app.route("/")
def home():

    return render_template(
        "index.html"
    )


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
