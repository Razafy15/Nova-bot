import os
import json
import time
import threading
import traceback

import numpy as np
import websocket

from flask import Flask, render_template, request, jsonify


# =========================================================
# FLASK
# =========================================================

app = Flask(__name__)


# =========================================================
# BOT CONFIG
# =========================================================

bot_config = {
    "app_id": "",
    "api_token": "",
    "account_id": "",
    "account_type": "demo",

    # Default market
    "symbol": "BOOM1000",

    # Trading
    "stake": 1.0,
    "is_running": False,
    "auto_trading": True,

    # Statistics
    "balance": 0.0,
    "currency": "USD",
    "market_price": 0.0,
    "total_trades": 0,
    "profit": 0.0,

    # Signal
    "pre_signal": "WAIT",
    "confirmed_signal": "WAIT",
    "confidence": 0,

    # Chart
    "ui_candles": [],

    # Connection
    "connected": False,
    "authorized": False,
    "status": "Disconnected",
    "last_error": "",

    # Strategy
    "paused_for_profit": False,

    # Available markets
    "markets": [],

    # Last update
    "last_update": 0
}


# =========================================================
# GLOBALS
# =========================================================

ws_app = None
ws_lock = threading.Lock()

active_contract_id = None

ws_thread = None

current_candle_subscription = None

request_counter = 100


# =========================================================
# REQUEST ID
# =========================================================

def next_req_id():
    global request_counter

    request_counter += 1

    return request_counter


# =========================================================
# SAFE SEND
# =========================================================

def send_ws(data):

    global ws_app

    try:

        if (
            ws_app
            and ws_app.sock
            and ws_app.sock.connected
        ):

            with ws_lock:

                ws_app.send(
                    json.dumps(data)
                )

            return True

    except Exception as e:

        print("WS SEND ERROR:", e)

        bot_config["last_error"] = str(e)

    return False


# =========================================================
# RSI
# =========================================================

def calculate_rsi(prices, period=14):

    try:

        prices = np.asarray(prices, dtype=float)

        if len(prices) < period + 1:
            return 50.0

        deltas = np.diff(prices)

        gains = np.where(
            deltas > 0,
            deltas,
            0
        )

        losses = np.where(
            deltas < 0,
            -deltas,
            0
        )

        avg_gain = np.mean(
            gains[-period:]
        )

        avg_loss = np.mean(
            losses[-period:]
        )

        if avg_loss == 0:

            if avg_gain == 0:
                return 50.0

            return 100.0

        rs = avg_gain / avg_loss

        rsi = 100 - (
            100 / (1 + rs)
        )

        return float(rsi)

    except Exception:

        return 50.0


# =========================================================
# EMA
# =========================================================

def calculate_ema(prices, period):

    try:

        prices = np.asarray(
            prices,
            dtype=float
        )

        if len(prices) == 0:
            return 0.0

        if len(prices) < period:
            return float(prices[-1])

        alpha = 2 / (period + 1)

        ema = prices[0]

        for price in prices[1:]:

            ema = (
                alpha * price
                + (1 - alpha) * ema
            )

        return float(ema)

    except Exception:

        return 0.0


# =========================================================
# MARKET TYPE
# =========================================================

def market_type(symbol):

    s = symbol.upper()

    if "BOOM" in s:
        return "BOOM"

    if "CRASH" in s:
        return "CRASH"

    if (
        "1HZ" in s
        or s.startswith("R_")
    ):
        return "VOLATILITY"

    return "OTHER"


# =========================================================
# REQUEST MARKETS
# =========================================================

def request_active_markets():

    send_ws({
        "active_symbols": "brief",
        "product_type": "basic",
        "req_id": next_req_id()
    })


# =========================================================
# REQUEST BALANCE
# =========================================================

def request_balance():

    send_ws({
        "balance": 1,
        "subscribe": 1,
        "req_id": next_req_id()
    })


# =========================================================
# REQUEST CHART
# =========================================================

def request_market_data():

    symbol = bot_config["symbol"]

    bot_config["ui_candles"] = []

    bot_config["market_price"] = 0.0

    bot_config["last_error"] = ""

    print(
        "Requesting chart:",
        symbol
    )

    # Stop old candle subscriptions
    try:

        send_ws({
            "forget_all": "candles"
        })

    except Exception:
        pass

    # Historical + streaming candles
    send_ws({

        "ticks_history": symbol,

        "adjust_start_time": 1,

        "count": 200,

        "end": "latest",

        "style": "candles",

        "granularity": 60,

        "subscribe": 1,

        "req_id": next_req_id()
    })


# =========================================================
# HANDLE CANDLES
# =========================================================

def process_candles(candles):

    formatted = []

    for candle in candles:

        try:

            formatted.append({

                "time": int(
                    candle["epoch"]
                ),

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

            print(
                "Candle error:",
                e
            )

    formatted.sort(
        key=lambda x: x["time"]
    )

    # Remove duplicates
    unique = {}

    for c in formatted:

        unique[c["time"]] = c

    formatted = list(
        unique.values()
    )

    formatted.sort(
        key=lambda x: x["time"]
    )

    # Keep last 300 candles
    formatted = formatted[-300:]

    bot_config[
        "ui_candles"
    ] = formatted

    if formatted:

        bot_config[
            "market_price"
        ] = formatted[-1]["close"]

    check_strategy_and_trade(
        formatted
    )


# =========================================================
# LIVE OHLC
# =========================================================

def process_ohlc(ohlc):

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

        candles = bot_config[
            "ui_candles"
        ]

        if candles:

            if (
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

        else:

            candles.append(
                candle
            )

        # Keep chart size reasonable
        if len(candles) > 300:

            del candles[:-300]

        bot_config[
            "market_price"
        ] = candle["close"]

        bot_config[
            "last_update"
        ] = time.time()

        check_strategy_and_trade(
            candles
        )

    except Exception as e:

        print(
            "OHLC ERROR:",
            e
        )


# =========================================================
# TICK
# =========================================================

def process_tick(tick):

    try:

        quote = float(
            tick["quote"]
        )

        bot_config[
            "market_price"
        ] = quote

        bot_config[
            "last_update"
        ] = time.time()

    except Exception as e:

        print(
            "TICK ERROR:",
            e
        )


# =========================================================
# STRATEGY
# =========================================================

def check_strategy_and_trade(candles):

    global active_contract_id

    if len(candles) < 50:

        bot_config[
            "pre_signal"
        ] = "WAIT"

        bot_config[
            "confirmed_signal"
        ] = "WAIT"

        bot_config[
            "confidence"
        ] = 0

        return

    symbol = bot_config[
        "symbol"
    ].upper()

    closes = np.array(
        [
            c["close"]
            for c in candles
        ],
        dtype=float
    )

    rsi = calculate_rsi(
        closes,
        14
    )

    ema20 = calculate_ema(
        closes,
        20
    )

    ema50 = calculate_ema(
        closes,
        50
    )

    # =====================================================
    # VOLATILITY
    # SIGNAL ONLY
    # =====================================================

    if market_type(symbol) == "VOLATILITY":

        bot_config[
            "pre_signal"
        ] = "WAIT"

        bot_config[
            "confirmed_signal"
        ] = "WAIT"

        bot_config[
            "confidence"
        ] = 0

        # Trend signal
        if ema20 > ema50:

            bot_config[
                "pre_signal"
            ] = "CALL"

            bot_config[
                "confidence"
            ] = min(
                95,
                int(
                    50
                    + abs(
                        ema20 - ema50
                    )
                    / max(
                        abs(ema50),
                        0.000001
                    )
                    * 1000
                )
            )

        elif ema20 < ema50:

            bot_config[
                "pre_signal"
            ] = "PUT"

            bot_config[
                "confidence"
            ] = min(
                95,
                int(
                    50
                    + abs(
                        ema20 - ema50
                    )
                    / max(
                        abs(ema50),
                        0.000001
                    )
                    * 1000
                )
            )

        return

    # =====================================================
    # BOOM
    # =====================================================

    if "BOOM" in symbol:

        recent = candles[-5:]

        green_count = sum(
            1
            for c in recent
            if c["close"] > c["open"]
        )

        # PRE SIGNAL
        if rsi >= 65:

            bot_config[
                "pre_signal"
            ] = "PUT"

            bot_config[
                "confidence"
            ] = min(
                90,
                int(rsi)
            )

        else:

            bot_config[
                "pre_signal"
            ] = "WAIT"

            bot_config[
                "confidence"
            ] = 0

        # CONFIRMED
        if (
            rsi >= 80
            and green_count >= 2
        ):

            bot_config[
                "confirmed_signal"
            ] = "PUT"

            bot_config[
                "confidence"
            ] = min(
                99,
                int(
                    rsi
                    + green_count * 3
                )
            )

            # AUTO TRADING
            if (
                bot_config["is_running"]
                and bot_config["auto_trading"]
                and active_contract_id is None
            ):

                request_trade(
                    "PUT"
                )

        else:

            bot_config[
                "confirmed_signal"
            ] = "WAIT"

        return

    # =====================================================
    # CRASH
    # =====================================================

    if "CRASH" in symbol:

        recent = candles[-5:]

        red_count = sum(
            1
            for c in recent
            if c["close"] < c["open"]
        )

        # PRE SIGNAL
        if rsi <= 35:

            bot_config[
                "pre_signal"
            ] = "CALL"

            bot_config[
                "confidence"
            ] = min(
                90,
                int(
                    100 - rsi
                )
            )

        else:

            bot_config[
                "pre_signal"
            ] = "WAIT"

            bot_config[
                "confidence"
            ] = 0

        # CONFIRMED
        if (
            rsi <= 20
            and red_count >= 2
        ):

            bot_config[
                "confirmed_signal"
            ] = "CALL"

            bot_config[
                "confidence"
            ] = min(
                99,
                int(
                    100 - rsi
                    + red_count * 3
                )
            )

            # AUTO TRADING
            if (
                bot_config["is_running"]
                and bot_config["auto_trading"]
                and active_contract_id is None
            ):

                request_trade(
                    "CALL"
                )

        else:

            bot_config[
                "confirmed_signal"
            ] = "WAIT"

        return

    # =====================================================
    # OTHER
    # =====================================================

    bot_config[
        "pre_signal"
    ] = "WAIT"

    bot_config[
        "confirmed_signal"
    ] = "WAIT"

    bot_config[
        "confidence"
    ] = 0


# =========================================================
# REQUEST TRADE PROPOSAL
# =========================================================

def request_trade(contract_type):

    symbol = bot_config[
        "symbol"
    ]

    stake = bot_config[
        "stake"
    ]

    print(
        "REQUEST TRADE:",
        contract_type,
        symbol,
        stake
    )

    # Boom / Crash options
    send_ws({

        "proposal": 1,

        "amount": stake,

        "basis": "stake",

        "currency": "USD",

        "symbol": symbol,

        "contract_type": contract_type,

        "duration": 5,

        "duration_unit": "t",

        "req_id": next_req_id()
    })


# =========================================================
# WEBSOCKET MESSAGE
# =========================================================

def on_message(ws, message):

    global active_contract_id

    try:

        data = json.loads(
            message
        )

        msg_type = data.get(
            "msg_type"
        )

        # =================================================
        # ERROR
        # =================================================

        if msg_type == "error":

            error = data.get(
                "error",
                {}
            )

            message_text = error.get(
                "message",
                "Unknown Deriv error"
            )

            print(
                "DERIV ERROR:",
                message_text
            )

            bot_config[
                "last_error"
            ] = message_text

            return

        # =================================================
        # AUTHORIZE
        # =================================================

        if msg_type == "authorize":

            print(
                "AUTHORIZED"
            )

            bot_config[
                "authorized"
            ] = True

            bot_config[
                "connected"
            ] = True

            bot_config[
                "status"
            ] = "Deriv Connected"

            bot_config[
                "last_error"
            ] = ""

            request_balance()

            request_active_markets()

            request_market_data()

            return

        # =================================================
        # BALANCE
        # =================================================

        if msg_type == "balance":

            balance_data = data.get(
                "balance",
                {}
            )

            if isinstance(
                balance_data,
                dict
            ):

                try:

                    bot_config[
                        "balance"
                    ] = float(
                        balance_data.get(
                            "balance",
                            0
                        )
                    )

                except Exception:

                    pass

                bot_config[
                    "currency"
                ] = balance_data.get(
                    "currency",
                    "USD"
                )

            return

        # =================================================
        # ACTIVE SYMBOLS
        # =================================================

        if msg_type == "active_symbols":

            markets = []

            for market in data.get(
                "active_symbols",
                []
            ):

                try:

                    symbol = market.get(
                        "symbol",
                        ""
                    )

                    display = market.get(
                        "display_name",
                        symbol
                    )

                    if not symbol:
                        continue

                    mtype = market_type(
                        symbol
                    )

                    if mtype in [
                        "BOOM",
                        "CRASH",
                        "VOLATILITY"
                    ]:

                        markets.append({

                            "symbol": symbol,

                            "display_name": display,

                            "type": mtype
                        })

                except Exception:
                    continue

            markets.sort(
                key=lambda x: (
                    x["type"],
                    x["display_name"]
                )
            )

            bot_config[
                "markets"
            ] = markets

            print(
                "MARKETS FOUND:",
                len(markets)
            )

            return

        # =================================================
        # CANDLES
        # =================================================

        if msg_type == "candles":

            candles = data.get(
                "candles",
                []
            )

            print(
                "CANDLES:",
                len(candles)
            )

            process_candles(
                candles
            )

            return

        # =================================================
        # OHLC
        # =================================================

        if msg_type == "ohlc":

            ohlc = data.get(
                "ohlc"
            )

            if ohlc:

                process_ohlc(
                    ohlc
                )

            return

        # =================================================
        # TICK
        # =================================================

        if msg_type == "tick":

            tick = data.get(
                "tick"
            )

            if tick:

                process_tick(
                    tick
                )

            return

        # =================================================
        # PROPOSAL
        # =================================================

        if msg_type == "proposal":

            proposal = data.get(
                "proposal"
            )

            if not proposal:
                return

            if (
                bot_config["is_running"]
                and bot_config["auto_trading"]
                and active_contract_id is None
            ):

                proposal_id = proposal.get(
                    "id"
                )

                if proposal_id:

                    print(
                        "BUY:",
                        proposal_id
                    )

                    send_ws({

                        "buy": proposal_id,

                        "price": bot_config[
                            "stake"
                        ],

                        "req_id": next_req_id()
                    })

            return

        # =================================================
        # BUY
        # =================================================

        if msg_type == "buy":

            buy = data.get(
                "buy"
            )

            if not buy:
                return

            active_contract_id = buy.get(
                "contract_id"
            )

            bot_config[
                "total_trades"
            ] += 1

            bot_config[
                "last_error"
            ] = ""

            print(
                "TRADE OPEN:",
                active_contract_id
            )

            if active_contract_id:

                send_ws({

                    "proposal_open_contract": 1,

                    "contract_id":
                        active_contract_id,

                    "subscribe": 1,

                    "req_id":
                        next_req_id()
                })

            return

        # =================================================
        # CONTRACT
        # =================================================

        if msg_type == "proposal_open_contract":

            poc = data.get(
                "proposal_open_contract"
            )

            if not poc:
                return

            status = poc.get(
                "status"
            )

            if status in [
                "won",
                "lost"
            ]:

                try:

                    profit = float(
                        poc.get(
                            "profit",
                            0
                        )
                    )

                except Exception:

                    profit = 0.0

                bot_config[
                    "profit"
                ] += profit

                print(
                    "TRADE CLOSED:",
                    status,
                    profit
                )

                active_contract_id = None

            return

    except Exception as e:

        print(
            "MESSAGE ERROR:",
            e
        )

        traceback.print_exc()

        bot_config[
            "last_error"
        ] = str(e)


# =========================================================
# WS ERROR
# =========================================================

def on_error(ws, error):

    print(
        "WEBSOCKET ERROR:",
        error
    )

    bot_config[
        "connected"
    ] = False

    bot_config[
        "authorized"
    ] = False

    bot_config[
        "status"
    ] = "Connection Error"

    bot_config[
        "last_error"
    ] = str(error)


# =========================================================
# WS CLOSE
# =========================================================

def on_close(
    ws,
    close_status_code,
    close_msg
):

    print(
        "WEBSOCKET CLOSED:",
        close_status_code,
        close_msg
    )

    bot_config[
        "connected"
    ] = False

    bot_config[
        "authorized"
    ] = False

    bot_config[
        "status"
    ] = "Disconnected"


# =========================================================
# WS OPEN
# =========================================================

def on_open(ws):

    print(
        "WEBSOCKET CONNECTED"
    )

    bot_config[
        "connected"
    ] = True

    bot_config[
        "status"
    ] = "Connected - Authorizing..."
    
    token = bot_config[
        "api_token"
    ]

    send_ws({

        "authorize": token,

        "req_id": next_req_id()
    })


# =========================================================
# RUN WS
# =========================================================

def run_websocket_bot():

    global ws_app

    app_id = bot_config[
        "app_id"
    ]

    if not app_id:

        return

    socket_url = (
        "wss://ws.derivws.com/"
        "websockets/v3"
        f"?app_id={app_id}"
    )

    print(
        "CONNECTING:",
        socket_url
    )

    websocket.enableTrace(False)

    ws_app = websocket.WebSocketApp(

        socket_url,

        on_open=on_open,

        on_message=on_message,

        on_error=on_error,

        on_close=on_close
    )

    try:

        ws_app.run_forever(
            ping_interval=25,
            ping_timeout=10
        )

    except Exception as e:

        print(
            "RUN FOREVER ERROR:",
            e
        )

        bot_config[
            "last_error"
        ] = str(e)


# =========================================================
# INDEX
# =========================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


# =========================================================
# CONFIGURE
# =========================================================

@app.route(
    "/api/configure",
    methods=["POST"]
)
def configure_bot():

    global ws_app
    global ws_thread

    data = request.get_json(
        silent=True
    ) or {}

    app_id = str(
        data.get(
            "app_id",
            ""
        )
    ).strip()

    token = str(
        data.get(
            "api_token",
            ""
        )
    ).strip()

    account_id = str(
        data.get(
            "account_id",
            ""
        )
    ).strip()

    account_type = str(
        data.get(
            "account_type",
            "demo"
        )
    ).lower()

    symbol = str(
        data.get(
            "symbol",
            "BOOM1000"
        )
    ).upper()

    try:

        stake = float(
            data.get(
                "stake",
                1
            )
        )

    except Exception:

        stake = 1.0

    if not app_id:

        return jsonify({
            "status": "error",
            "message": "Ampidiro ny App ID."
        }), 400

    if not token:

        return jsonify({
            "status": "error",
            "message": "Ampidiro ny API Token."
        }), 400

    # Stop old connection
    if ws_app:

        try:

            ws_app.close()

        except Exception:
            pass

    bot_config[
        "app_id"
    ] = app_id

    bot_config[
        "api_token"
    ] = token

    bot_config[
        "account_id"
    ] = account_id

    bot_config[
        "account_type"
    ] = account_type

    bot_config[
        "symbol"
    ] = symbol

    bot_config[
        "stake"
    ] = max(
        0.01,
        stake
    )

    bot_config[
        "connected"
    ] = False

    bot_config[
        "authorized"
    ] = False

    bot_config[
        "status"
    ] = "Connecting..."

    bot_config[
        "last_error"
    ] = ""

    ws_thread = threading.Thread(
        target=run_websocket_bot,
        daemon=True
    )

    ws_thread.start()

    return jsonify({

        "status": "success",

        "message":
            "Configuration voaray. "
            "Mampifandray amin'i Deriv..."
    })


# =========================================================
# START
# =========================================================

@app.route(
    "/api/start",
    methods=["POST"]
)
def start_bot():

    data = request.get_json(
        silent=True
    ) or {}

    try:

        stake = float(
            data.get(
                "stake",
                bot_config["stake"]
            )
        )

    except Exception:

        stake = bot_config[
            "stake"
        ]

    symbol = str(
        data.get(
            "symbol",
            bot_config["symbol"]
        )
    ).upper()

    bot_config[
        "stake"
    ] = max(
        0.01,
        stake
    )

    bot_config[
        "symbol"
    ] = symbol

    bot_config[
        "is_running"
    ] = True

    bot_config[
        "paused_for_profit"
    ] = False

    bot_config[
        "last_error"
    ] = ""

    bot_config[
        "pre_signal"
    ] = "WAIT"

    bot_config[
        "confirmed_signal"
    ] = "WAIT"

    if bot_config["authorized"]:

        request_market_data()

    return jsonify({

        "status": "success",

        "message":
            "Bot STARTED."
    })


# =========================================================
# STOP
# =========================================================

@app.route(
    "/api/stop",
    methods=["POST"]
)
def stop_bot():

    bot_config[
        "is_running"
    ] = False

    bot_config[
        "pre_signal"
    ] = "WAIT"

    bot_config[
        "confirmed_signal"
    ] = "WAIT"

    return jsonify({

        "status": "success",

        "message":
            "Bot STOPPED."
    })


# =========================================================
# CHANGE SYMBOL
# =========================================================

@app.route(
    "/api/symbol",
    methods=["POST"]
)
def change_symbol():

    data = request.get_json(
        silent=True
    ) or {}

    symbol = str(
        data.get(
            "symbol",
            ""
        )
    ).upper()

    if not symbol:

        return jsonify({
            "status": "error",
            "message": "Symbol tsy misy."
        }), 400

    bot_config[
        "symbol"
    ] = symbol

    bot_config[
        "pre_signal"
    ] = "WAIT"

    bot_config[
        "confirmed_signal"
    ] = "WAIT"

    if bot_config["authorized"]:

        request_market_data()

    return jsonify({

        "status": "success",

        "symbol": symbol
    })


# =========================================================
# STATS
# =========================================================

@app.route(
    "/api/stats",
    methods=["GET"]
)
def stats():

    return jsonify({

        "connected":
            bot_config["connected"],

        "authorized":
            bot_config["authorized"],

        "status":
            bot_config["status"],

        "balance":
            bot_config["balance"],

        "currency":
            bot_config["currency"],

        "market_price":
            bot_config["market_price"],

        "total_trades":
            bot_config["total_trades"],

        "profit":
            bot_config["profit"],

        "symbol":
            bot_config["symbol"],

        "pre_signal":
            bot_config["pre_signal"],

        "confirmed_signal":
            bot_config[
                "confirmed_signal"
            ],

        "confidence":
            bot_config["confidence"],

        "is_running":
            bot_config["is_running"],

        "auto_trading":
            bot_config["auto_trading"],

        "ui_candles":
            bot_config[
                "ui_candles"
            ],

        "markets":
            bot_config["markets"],

        "last_error":
            bot_config["last_error"]
    })


# =========================================================
# RUN
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
        port=port,
        debug=False
    )
