import json
import os
import threading
import time

import requests
import websocket

from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

REST_BASE = "https://api.derivws.com"

ws = None
ws_thread = None
trading_thread = None

ws_lock = threading.Lock()
state_lock = threading.Lock()

APP_ID = ""
PAT_TOKEN = ""

state = {
    "connected": False,
    "authorized": False,

    "balance": 0.0,
    "currency": "USD",

    "account_id": "",
    "account_type": "",

    "symbol": "",
    "symbol_display": "",

    "last_price": None,
    "ticks_buffer": [],

    "running": False,

    "stake": 0.35,
    "duration": 3,
    "duration_unit": "s",

    "direction": "PUT",

    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "profit": 0.0,
    "loss_streak": 0,

    "current_trade": None,

    "boom_symbols": [],
    "available_contracts": [],

    "history": [],
    "logs": [],

    "last_error": "",

    "last_proposal": None,
    "last_trade_time": 0,

    "trade_interval": 5,
}


# ============================================================
# LOG
# ============================================================

def add_log(message):
    timestamp = time.strftime("%H:%M:%S")

    with state_lock:
        state["logs"].insert(
            0,
            {
                "time": timestamp,
                "message": str(message)
            }
        )

        state["logs"] = state["logs"][:100]

    print(f"[{timestamp}] {message}", flush=True)


# ============================================================
# WEBSOCKET SEND
# ============================================================

def send_ws(payload):
    global ws

    with ws_lock:
        current_ws = ws

    if current_ws is None:
        add_log("WS SEND FAILED: WebSocket is not initialized.")
        return False

    try:
        if not current_ws.sock or not current_ws.sock.connected:
            add_log("WS SEND FAILED: WebSocket is not connected.")
            return False

        current_ws.send(json.dumps(payload))
        return True

    except Exception as exc:
        state["last_error"] = str(exc)
        add_log(f"WS SEND ERROR: {exc}")
        return False


# ============================================================
# REST ERROR
# ============================================================

def api_error(response):
    try:
        data = response.json()
    except Exception:
        return f"HTTP {response.status_code}: {response.text[:500]}"

    errors = data.get("errors")

    if isinstance(errors, list) and errors:
        err = errors[0]

        return (
            f"HTTP {response.status_code} | "
            f"{err.get('code', 'ERROR')} | "
            f"{err.get('message', 'Unknown error')}"
        )

    return f"HTTP {response.status_code}: {json.dumps(data)[:500]}"


# ============================================================
# GET OPTIONS ACCOUNTS
# ============================================================

def get_accounts(app_id, token):
    url = REST_BASE + "/trading/v1/options/accounts"

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=20
    )

    if not response.ok:
        raise RuntimeError(api_error(response))

    data = response.json()

    accounts = data.get("data", [])

    if isinstance(accounts, dict):
        accounts = [accounts]

    if not isinstance(accounts, list):
        accounts = []

    return accounts


# ============================================================
# GET OTP WEBSOCKET URL
# ============================================================

def get_otp_url(app_id, token, account_id):
    url = (
        REST_BASE
        + "/trading/v1/options/accounts/"
        + account_id
        + "/otp"
    )

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    response = requests.post(
        url,
        headers=headers,
        timeout=20
    )

    if not response.ok:
        raise RuntimeError(api_error(response))

    data = response.json()

    ws_url = (
        data
        .get("data", {})
        .get("url")
    )

    if not ws_url:
        raise RuntimeError(
            "OTP response did not contain data.url"
        )

    return ws_url


# ============================================================
# WEBSOCKET OPEN
# ============================================================

def on_open(socket):
    state["connected"] = True
    state["last_error"] = ""

    add_log("================================")
    add_log("DERIV WEBSOCKET CONNECTED")
    add_log("================================")

    # Balance
    send_ws({
        "balance": 1,
        "subscribe": 1,
        "req_id": 100
    })

    # Active symbols
    send_ws({
        "active_symbols": "full",
        "req_id": 101
    })


# ============================================================
# MESSAGE
# ============================================================

def on_message(socket, message):

    try:
        data = json.loads(message)

    except Exception as exc:
        add_log(f"Invalid JSON: {exc}")
        return

    msg_type = data.get("msg_type")

    # --------------------------------------------------------
    # ERROR
    # --------------------------------------------------------

    if msg_type == "error":

        error = data.get("error", {})

        code = error.get(
            "code",
            "UNKNOWN"
        )

        message_text = error.get(
            "message",
            "Unknown Deriv error"
        )

        state["last_error"] = (
            f"{code}: {message_text}"
        )

        add_log(
            f"DERIV ERROR: {code}: {message_text}"
        )

        return

    # --------------------------------------------------------
    # BALANCE
    # --------------------------------------------------------

    if msg_type == "balance":

        balance = data.get(
            "balance",
            {}
        )

        try:
            state["balance"] = float(
                balance.get(
                    "balance",
                    0
                )
            )

        except Exception:
            state["balance"] = 0.0

        state["currency"] = (
            balance.get("currency")
            or "USD"
        )

        state["authorized"] = True

        add_log(
            "AUTHORIZED | "
            f"Balance: {state['balance']:.2f} "
            f"{state['currency']}"
        )

        return

    # --------------------------------------------------------
    # ACTIVE SYMBOLS
    # --------------------------------------------------------

    if msg_type == "active_symbols":

        symbols = data.get(
            "active_symbols",
            []
        )

        boom = []

        for item in symbols:

            symbol = str(
                item.get(
                    "underlying_symbol",
                    ""
                )
            ).strip()

            display_name = str(
                item.get(
                    "display_name",
                    ""
                )
            ).strip()

            text = (
                symbol
                + " "
                + display_name
            ).upper()

            # IMPORTANT:
            # BOOM ihany no raisina.
            # Tsy R_ rehetra intsony.

            if "BOOM" in text:

                boom.append({
                    "symbol": symbol,
                    "display_name": (
                        display_name
                        or symbol
                    )
                })

        boom.sort(
            key=lambda x: x["symbol"]
        )

        state["boom_symbols"] = boom

        add_log(
            f"BOOM symbols found: {len(boom)}"
        )

        for item in boom:
            add_log(
                f"BOOM: "
                f"{item['symbol']} = "
                f"{item['display_name']}"
            )

        return

    # --------------------------------------------------------
    # TICK
    # --------------------------------------------------------

    if msg_type == "tick":

        tick = data.get(
            "tick",
            {}
        )

        try:

            price = float(
                tick.get(
                    "quote"
                )
            )

            state["last_price"] = price

            state["ticks_buffer"].append(
                price
            )

            if len(
                state["ticks_buffer"]
            ) > 100:

                state["ticks_buffer"].pop(
                    0
                )

        except Exception:
            pass

        return

    # --------------------------------------------------------
    # CONTRACTS FOR
    # --------------------------------------------------------

    if msg_type == "contracts_for":

        result = data.get(
            "contracts_for",
            {}
        )

        available = result.get(
            "available",
            []
        )

        if not isinstance(
            available,
            list
        ):
            available = []

        state["available_contracts"] = (
            available
        )

        add_log(
            f"Contracts received: "
            f"{len(available)}"
        )

        return

    # --------------------------------------------------------
    # PROPOSAL
    # --------------------------------------------------------

    if msg_type == "proposal":

        proposal = data.get(
            "proposal",
            {}
        )

        proposal_id = proposal.get(
            "id"
        )

        if not proposal_id:
            add_log(
                "Proposal received without ID."
            )
            return

        ask_price = proposal.get(
            "ask_price"
        )

        if ask_price is None:
            ask_price = proposal.get(
                "price"
            )

        try:
            ask_price = float(
                ask_price
            )

        except Exception:
            ask_price = None

        state["last_proposal"] = {
            "id": proposal_id,
            "price": ask_price,
            "received_at": time.time()
        }

        add_log(
            f"PROPOSAL RECEIVED | "
            f"ID={proposal_id} | "
            f"ASK={ask_price}"
        )

        return

    # --------------------------------------------------------
    # BUY
    # --------------------------------------------------------

    if msg_type == "buy":

        buy = data.get(
            "buy",
            {}
        )

        contract_id = buy.get(
            "contract_id"
        )

        if not contract_id:

            add_log(
                "BUY response received "
                "without contract_id."
            )

            return

        state["current_trade"] = {
            "contract_id": contract_id,
            "symbol": state["symbol"],
            "direction": state["direction"],
            "stake": state["stake"],
            "status": "OPEN"
        }

        state["last_trade_time"] = (
            time.time()
        )

        add_log(
            f"CONTRACT OPENED: "
            f"{contract_id}"
        )

        send_ws({
            "proposal_open_contract": 1,
            "contract_id": contract_id,
            "subscribe": 1,
            "req_id": 300
        })

        return

    # --------------------------------------------------------
    # OPEN CONTRACT
    # --------------------------------------------------------

    if msg_type == "proposal_open_contract":

        contract = data.get(
            "proposal_open_contract",
            {}
        )

        contract_id = contract.get(
            "contract_id"
        )

        if (
            not contract.get("is_sold")
            and not contract.get("is_expired")
        ):
            return

        try:
            profit = float(
                contract.get(
                    "profit",
                    0
                )
            )

        except Exception:
            profit = 0.0

        state["profit"] += profit
        state["total_trades"] += 1

        if profit > 0:

            state["wins"] += 1
            state["loss_streak"] = 0

            result = "WIN"

        else:

            state["losses"] += 1
            state["loss_streak"] += 1

            result = "LOSS"

        state["history"].insert(
            0,
            {
                "time": time.strftime(
                    "%H:%M:%S"
                ),

                "symbol": (
                    state["symbol_display"]
                    or state["symbol"]
                ),

                "direction": (
                    state["direction"]
                ),

                "stake": state["stake"],

                "result": result,

                "profit": profit
            }
        )

        state["history"] = (
            state["history"][:100]
        )

        state["current_trade"] = None

        add_log(
            f"TRADE CLOSED | "
            f"{result} | "
            f"Profit={profit:+.2f}"
        )

        return


# ============================================================
# WS ERROR
# ============================================================

def on_error(socket, error):

    state["connected"] = False
    state["authorized"] = False
    state["last_error"] = str(error)

    add_log(
        f"WEBSOCKET ERROR: {error}"
    )


# ============================================================
# WS CLOSE
# ============================================================

def on_close(socket, code, reason):

    state["connected"] = False
    state["authorized"] = False

    add_log(
        f"WEBSOCKET CLOSED: "
        f"{code} {reason}"
    )


# ============================================================
# WS THREAD
# ============================================================

def websocket_thread(ws_url):

    global ws

    try:

        add_log(
            "Opening authenticated "
            "WebSocket..."
        )

        new_ws = websocket.WebSocketApp(
            ws_url,

            on_open=on_open,

            on_message=on_message,

            on_error=on_error,

            on_close=on_close
        )

        with ws_lock:
            ws = new_ws

        new_ws.run_forever(
            ping_interval=20,
            ping_timeout=10
        )

    except Exception as exc:

        state["connected"] = False
        state["authorized"] = False
        state["last_error"] = str(exc)

        add_log(
            f"WS THREAD ERROR: {exc}"
        )

    finally:

        with ws_lock:
            ws = None


# ============================================================
# TRADING LOOP
# ============================================================

def trading_loop():

    add_log(
        "=== TRADING LOOP STARTED ==="
    )

    while state["running"]:

        try:

            # Must be connected
            if not state["connected"]:
                time.sleep(1)
                continue

            # Must be authorized
            if not state["authorized"]:
                time.sleep(1)
                continue

            # Must have symbol
            if not state["symbol"]:
                time.sleep(1)
                continue

            # Don't open another trade
            if state["current_trade"] is not None:
                time.sleep(0.5)
                continue

            # Interval
            now = time.time()

            if (
                now
                - state["last_trade_time"]
                < state["trade_interval"]
            ):
                time.sleep(0.25)
                continue

            # ------------------------------------------------
            # BASIC ENTRY
            # ------------------------------------------------

            state["direction"] = "PUT"

            symbol = state["symbol"]

            add_log(
                f"ENTRY SIGNAL: "
                f"PUT on {symbol}"
            )

            # ------------------------------------------------
            # NEW DERIV API PROPOSAL
            # IMPORTANT:
            # underlying_symbol
            # duration_unit = s
            # basis = stake
            # ------------------------------------------------

            proposal_payload = {

                "proposal": 1,

                "amount": float(
                    state["stake"]
                ),

                "basis": "stake",

                "contract_type": "PUT",

                "currency": state[
                    "currency"
                ],

                "duration": int(
                    state["duration"]
                ),

                "duration_unit": state[
                    "duration_unit"
                ],

                "underlying_symbol": symbol,

                "subscribe": 1,

                "req_id": int(
                    time.time() * 1000
                ) % 100000000
            }

            state["last_proposal"] = None

            sent = send_ws(
                proposal_payload
            )

            if not sent:

                add_log(
                    "Proposal was not sent."
                )

                time.sleep(1)
                continue

            # Wait for proposal
            started = time.time()

            while (
                time.time() - started < 5
            ):

                proposal = (
                    state["last_proposal"]
                )

                if proposal:

                    break

                time.sleep(0.1)

            proposal = state[
                "last_proposal"
            ]

            if not proposal:

                add_log(
                    "PROPOSAL TIMEOUT."
                )

                state[
                    "last_trade_time"
                ] = time.time()

                continue

            proposal_id = proposal.get(
                "id"
            )

            price = proposal.get(
                "price"
            )

            if not proposal_id:

                add_log(
                    "Proposal ID missing."
                )

                continue

            if price is None:

                add_log(
                    "Proposal ask price missing."
                )

                continue

            # ------------------------------------------------
            # BUY
            # ------------------------------------------------

            buy_payload = {

                "buy": proposal_id,

                "price": float(price),

                "req_id": int(
                    time.time() * 1000
                ) % 100000000
            }

            add_log(
                f"BUYING proposal "
                f"{proposal_id} "
                f"price={price}"
            )

            sent = send_ws(
                buy_payload
            )

            if sent:

                state[
                    "last_trade_time"
                ] = time.time()

            else:

                add_log(
                    "BUY SEND FAILED."
                )

            time.sleep(0.5)

        except Exception as exc:

            add_log(
                f"TRADING LOOP ERROR: "
                f"{exc}"
            )

            time.sleep(2)

    add_log(
        "=== TRADING LOOP STOPPED ==="
    )


# ============================================================
# CONNECT
# ============================================================

@app.post("/api/connect")
def api_connect():

    global APP_ID
    global PAT_TOKEN
    global ws_thread

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

    token = str(
        data.get(
            "token",
            ""
        )
    ).strip()

    account_id = str(
        data.get(
            "account_id",
            ""
        )
    ).strip()

    if not app_id:

        return jsonify({
            "ok": False,
            "error": "App ID is required."
        }), 400

    if not token:

        return jsonify({
            "ok": False,
            "error": "PAT/API Token is required."
        }), 400

    if not account_id:

        return jsonify({
            "ok": False,
            "error": "Account ID is required."
        }), 400

    try:

        add_log(
            "========== CONNECT =========="
        )

        # --------------------------------------------
        # CHECK ACCOUNT
        # --------------------------------------------

        accounts = get_accounts(
            app_id,
            token
        )

        selected = None

        for account in accounts:

            aid = str(
                account.get(
                    "account_id",
                    ""
                )
            ).strip()

            if aid == account_id:

                selected = account
                break

        if selected is None:

            raise RuntimeError(
                "Account ID was not found "
                "for this PAT."
            )

        state["account_id"] = account_id

        state["account_type"] = (
            selected.get(
                "account_type",
                ""
            )
        )

        state["currency"] = (
            selected.get(
                "currency",
                "USD"
            )
        )

        add_log(
            f"Account found: "
            f"{account_id}"
        )

        add_log(
            f"Account type: "
            f"{state['account_type']}"
        )

        # --------------------------------------------
        # GET OTP
        # --------------------------------------------

        ws_url = get_otp_url(
            app_id,
            token,
            account_id
        )

        if not ws_url:

            raise RuntimeError(
                "Empty WebSocket URL."
            )

        add_log(
            "OTP WebSocket URL received."
        )

        APP_ID = app_id
        PAT_TOKEN = token

        # --------------------------------------------
        # CLOSE OLD WS
        # --------------------------------------------

        with ws_lock:
            old_ws = ws

        if old_ws is not None:

            try:
                old_ws.close()

            except Exception:
                pass

        # --------------------------------------------
        # RESET STATE
        # --------------------------------------------

        state["connected"] = False
        state["authorized"] = False
        state["last_error"] = ""
        state["last_proposal"] = None

        # --------------------------------------------
        # START WS
        # --------------------------------------------

        ws_thread = threading.Thread(
            target=websocket_thread,
            args=(ws_url,),
            daemon=True
        )

        ws_thread.start()

        add_log(
            "Authenticated WebSocket "
            "thread started."
        )

        return jsonify({
            "ok": True,
            "message": (
                "Connection started."
            )
        })

    except Exception as exc:

        state["connected"] = False
        state["authorized"] = False
        state["last_error"] = str(exc)

        add_log(
            f"CONNECT FAILED: {exc}"
        )

        return jsonify({
            "ok": False,
            "error": str(exc)
        }), 400


# ============================================================
# FIND BOOM
# ============================================================

@app.post("/api/markets")
def api_markets():

    if not state["connected"]:

        return jsonify({
            "ok": False,
            "error": (
                "Connect Deriv first."
            )
        }), 400

    ok = send_ws({
        "active_symbols": "full",
        "req_id": 500
    })

    if not ok:

        return jsonify({
            "ok": False,
            "error": (
                "WebSocket is not connected."
            )
        }), 400

    return jsonify({
        "ok": True
    })


# ============================================================
# SELECT SYMBOL
# ============================================================

@app.post("/api/select-symbol")
def api_select_symbol():

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

    display_name = str(
        data.get(
            "display_name",
            symbol
        )
    ).strip()

    if not symbol:

        return jsonify({
            "ok": False,
            "error": "Symbol is required."
        }), 400

    if not state["connected"]:

        return jsonify({
            "ok": False,
            "error": (
                "Connect Deriv first."
            )
        }), 400

    # Validate against returned BOOM list
    valid = any(
        item.get("symbol") == symbol
        for item in state["boom_symbols"]
    )

    if not valid:

        return jsonify({
            "ok": False,
            "error": (
                "This symbol is not "
                "in the current BOOM list."
            )
        }), 400

    state["symbol"] = symbol

    state["symbol_display"] = (
        display_name
        or symbol
    )

    state["available_contracts"] = []

    state["ticks_buffer"] = []

    state["last_price"] = None

    # Tick subscription
    send_ws({
        "ticks": symbol,
        "subscribe": 1,
        "req_id": 600
    })

    # Contract metadata
    send_ws({
        "contracts_for": symbol,
        "req_id": 601
    })

    add_log(
        f"SELECTED: "
        f"{symbol} "
        f"({display_name})"
    )

    return jsonify({
        "ok": True,
        "symbol": symbol,
        "display_name": display_name
    })


# ============================================================
# START
# ============================================================

@app.post("/api/start")
def api_start():

    global trading_thread

    add_log(
        "START BUTTON PRESSED."
    )

    if not state["connected"]:

        return jsonify({
            "ok": False,
            "error": (
                "WebSocket is not connected."
            )
        }), 400

    if not state["authorized"]:

        return jsonify({
            "ok": False,
            "error": (
                "Deriv account is not "
                "authorized yet."
            )
        }), 400

    if not state["symbol"]:

        return jsonify({
            "ok": False,
            "error": (
                "Select a BOOM symbol first."
            )
        }), 400

    state["running"] = True

    state["last_trade_time"] = 0

    if (
        trading_thread is None
        or not trading_thread.is_alive()
    ):

        trading_thread = threading.Thread(
            target=trading_loop,
            daemon=True
        )

        trading_thread.start()

        add_log(
            "Trading thread started."
        )

    add_log(
        "BOT STARTED."
    )

    return jsonify({
        "ok": True
    })


# ============================================================
# PAUSE
# ============================================================

@app.post("/api/pause")
def api_pause():

    state["running"] = False

    add_log(
        "BOT PAUSED."
    )

    return jsonify({
        "ok": True
    })


# ============================================================
# STOP
# ============================================================

@app.post("/api/stop")
def api_stop():

    state["running"] = False

    add_log(
        "BOT STOPPED."
    )

    return jsonify({
        "ok": True
    })


# ============================================================
# INTERVAL
# ============================================================

@app.post("/api/update-interval")
def api_update_interval():

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    try:

        interval = int(
            data.get(
                "interval",
                5
            )
        )

    except Exception:

        return jsonify({
            "ok": False,
            "error": "Invalid interval."
        }), 400

    if interval < 1:

        return jsonify({
            "ok": False,
            "error": (
                "Interval must be "
                "at least 1 second."
            )
        }), 400

    state["trade_interval"] = (
        interval
    )

    add_log(
        f"Trade interval: "
        f"{interval}s"
    )

    return jsonify({
        "ok": True,
        "interval": interval
    })


# ============================================================
# DURATION
# ============================================================

@app.post("/api/update-duration")
def api_update_duration():

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    try:

        duration = int(
            data.get(
                "duration",
                3
            )
        )

    except Exception:

        return jsonify({
            "ok": False,
            "error": "Invalid duration."
        }), 400

    if duration < 1:

        return jsonify({
            "ok": False,
            "error": (
                "Duration must be "
                "at least 1 second."
            )
        }), 400

    state["duration"] = duration

    state["duration_unit"] = "s"

    add_log(
        f"Duration: {duration}s"
    )

    return jsonify({
        "ok": True,
        "duration": duration,
        "duration_unit": "s"
    })


# ============================================================
# STATUS
# ============================================================

@app.get("/api/status")
def api_status():

    total = state["total_trades"]

    if total:

        win_rate = (
            state["wins"]
            / total
            * 100
        )

    else:

        win_rate = 0.0

    return jsonify({
        **state,
        "win_rate": win_rate
    })


# ============================================================
# INDEX
# ============================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/health")
def health():

    return jsonify({
        "ok": True,
        "service": "NOVA BOOM/FALL BOT",
        "websocket_connected": (
            state["connected"]
        ),
        "authorized": (
            state["authorized"]
        ),
        "running": (
            state["running"]
        )
    })


# ============================================================
# RUN
# ============================================================

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
