import json
import threading
import time

import requests
import websocket

from flask import Flask, jsonify, render_template, request


app = Flask(__name__)

# ============================================================
# DERIV CONFIG
# ============================================================

REST_BASE = "https://api.derivws.com"

APP_ID = ""
PAT_TOKEN = ""

ws = None
ws_thread = None

ws_lock = threading.Lock()


# ============================================================
# STATE
# ============================================================

state = {
    "connected": False,
    "authorized": False,

    "balance": 0.0,
    "currency": "USD",

    "account_id": "",
    "account_type": "",

    "running": False,

    "symbol": "",
    "last_price": None,

    "stake": 0.35,
    "duration": 3,

    "direction": "PUT/FALL",
    "martingale": False,

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
}


# ============================================================
# LOG
# ============================================================

def add_log(message):

    timestamp = time.strftime("%H:%M:%S")

    state["logs"].insert(
        0,
        {
            "time": timestamp,
            "message": str(message),
        }
    )

    state["logs"] = state["logs"][:100]

    print(
        f"[{timestamp}] {message}",
        flush=True
    )


# ============================================================
# RESET CONNECTION STATE
# ============================================================

def reset_connection_state():

    state["connected"] = False
    state["authorized"] = False

    state["balance"] = 0.0
    state["currency"] = "USD"

    state["account_type"] = ""

    state["last_price"] = None

    state["current_trade"] = None


# ============================================================
# REST ERROR PARSER
# ============================================================

def parse_rest_error(response):

    try:
        data = response.json()
    except Exception:
        return (
            f"HTTP {response.status_code}: "
            f"{response.text[:500]}"
        )

    errors = data.get("errors")

    if isinstance(errors, list) and errors:

        first = errors[0]

        code = first.get(
            "code",
            "UnknownError"
        )

        message = first.get(
            "message",
            "Unknown Deriv error"
        )

        return (
            f"HTTP {response.status_code} "
            f"{code}: {message}"
        )

    return (
        f"HTTP {response.status_code}: "
        f"{json.dumps(data)[:500]}"
    )


# ============================================================
# REST GET ACCOUNTS
# ============================================================

def get_accounts(app_id, token):

    url = (
        REST_BASE
        + "/trading/v1/options/accounts"
    )

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=20,
    )

    if not response.ok:

        raise RuntimeError(
            parse_rest_error(response)
        )

    data = response.json()

    accounts = data.get(
        "data",
        []
    )

    # API may return a list.
    if isinstance(accounts, dict):
        accounts = [accounts]

    if not isinstance(accounts, list):
        accounts = []

    return accounts


# ============================================================
# REST GET OTP WEBSOCKET URL
# ============================================================

def get_otp_url(
    app_id,
    token,
    account_id,
):

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
        timeout=20,
    )

    if not response.ok:

        raise RuntimeError(
            parse_rest_error(response)
        )

    data = response.json()

    payload = data.get(
        "data",
        {}
    )

    ws_url = payload.get(
        "url"
    )

    if not ws_url:

        raise RuntimeError(
            "Deriv OTP response did not "
            "contain a WebSocket URL."
        )

    return ws_url


# ============================================================
# CLOSE OLD WEBSOCKET
# ============================================================

def close_old_ws():

    global ws

    with ws_lock:

        old_ws = ws
        ws = None

    if old_ws is not None:

        try:
            old_ws.close()
        except Exception:
            pass


# ============================================================
# SEND WS
# ============================================================

def send_ws(payload):

    global ws

    with ws_lock:

        current_ws = ws

        if current_ws is None:

            add_log(
                "WS SEND: WebSocket is not initialized."
            )

            return False

        try:

            current_ws.send(
                json.dumps(payload)
            )

            return True

        except Exception as exc:

            state["last_error"] = str(exc)

            add_log(
                f"WS SEND ERROR: {exc}"
            )

            return False


# ============================================================
# WS OPEN
# ============================================================

def on_open(socket):

    state["connected"] = True
    state["last_error"] = ""

    add_log(
        "WebSocket connected successfully."
    )

    # Account balance
    send_ws({
        "balance": 1,
        "subscribe": 1,
        "req_id": 1,
    })

    # Market symbols
    send_ws({
        "active_symbols": "full",
        "req_id": 2,
    })


# ============================================================
# WS MESSAGE
# ============================================================

def on_message(socket, message):

    try:

        data = json.loads(message)

    except Exception as exc:

        add_log(
            f"Invalid WebSocket JSON: {exc}"
        )

        return

    msg_type = data.get(
        "msg_type"
    )

    # --------------------------------------------------------
    # API ERROR
    # --------------------------------------------------------

    if msg_type == "error":

        error = data.get(
            "error",
            {}
        )

        code = error.get(
            "code",
            "Unknown"
        )

        message_text = error.get(
            "message",
            "Unknown Deriv error"
        )

        state["last_error"] = (
            f"{code}: {message_text}"
        )

        add_log(
            f"DERIV ERROR: {code}: "
            f"{message_text}"
        )

        return

    # --------------------------------------------------------
    # BALANCE
    # --------------------------------------------------------

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

        except (
            TypeError,
            ValueError,
        ):

            state["balance"] = 0.0

        state["currency"] = (
            balance_data.get(
                "currency"
            )
            or "USD"
        )

        state["authorized"] = True
        state["last_error"] = ""

        add_log(
            "Account authorized. "
            f"Balance: "
            f"{state['balance']:.2f} "
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

        boom_symbols = []

        for item in symbols:

            if not isinstance(item, dict):
                continue

            symbol = str(
                item.get(
                    "underlying_symbol",
                    ""
                )
            ).strip()

            name = str(
                item.get(
                    "underlying_symbol_name",
                    ""
                )
            ).strip()

            symbol_type = str(
                item.get(
                    "underlying_symbol_type",
                    ""
                )
            ).strip()

            market = str(
                item.get(
                    "market",
                    ""
                )
            ).strip()

            search_text = (
                symbol
                + " "
                + name
            ).upper()

            if "BOOM" in search_text:

                boom_symbols.append({
                    "symbol": symbol,
                    "display_name": (
                        name or symbol
                    ),
                    "symbol_type": symbol_type,
                    "market": market,
                })

        # Remove duplicates
        unique = {}

        for item in boom_symbols:

            unique[
                item["symbol"]
            ] = item

        state["boom_symbols"] = list(
            unique.values()
        )

        add_log(
            f"Active symbols received: "
            f"{len(symbols)}"
        )

        add_log(
            f"BOOM symbols found: "
            f"{len(state['boom_symbols'])}"
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

            state["last_price"] = float(
                tick.get("quote")
            )

        except (
            TypeError,
            ValueError,
        ):

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
            f"Contracts received for "
            f"{state['symbol']}: "
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

        if proposal_id:

            add_log(
                f"Proposal received: "
                f"{proposal_id}"
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

        if contract_id:

            state["current_trade"] = {
                "contract_id": contract_id,
                "symbol": state["symbol"],
                "direction": state["direction"],
                "stake": state["stake"],
                "status": "OPEN",
            }

            add_log(
                f"Contract opened: "
                f"{contract_id}"
            )

            send_ws({
                "proposal_open_contract": 1,
                "contract_id": contract_id,
                "subscribe": 1,
                "req_id": 7,
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

        is_sold = contract.get(
            "is_sold"
        )

        is_expired = contract.get(
            "is_expired"
        )

        if is_sold or is_expired:

            try:

                profit = float(
                    contract.get(
                        "profit",
                        0
                    )
                )

            except (
                TypeError,
                ValueError,
            ):

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
                    "time":
                        time.strftime(
                            "%H:%M:%S"
                        ),
                    "symbol":
                        state["symbol"],
                    "direction":
                        state["direction"],
                    "stake":
                        state["stake"],
                    "result":
                        result,
                    "profit":
                        profit,
                }
            )

            state["history"] = (
                state["history"][:100]
            )

            state["current_trade"] = None

            add_log(
                f"{result}: "
                f"{profit:+.2f}"
            )

        return


# ============================================================
# WS ERROR
# ============================================================

def on_error(socket, error):

    state["connected"] = False
    state["authorized"] = False

    state["last_error"] = str(
        error
    )

    add_log(
        f"WebSocket error: {error}"
    )


# ============================================================
# WS CLOSE
# ============================================================

def on_close(
    socket,
    code,
    message,
):

    state["connected"] = False
    state["authorized"] = False

    add_log(
        f"WebSocket closed: "
        f"{code} {message}"
    )


# ============================================================
# WS THREAD
# ============================================================

def websocket_thread(ws_url):

    global ws

    try:

        new_ws = websocket.WebSocketApp(
            ws_url,

            on_open=on_open,

            on_message=on_message,

            on_error=on_error,

            on_close=on_close,
        )

        with ws_lock:
            ws = new_ws

        add_log(
            "Opening authenticated "
            "Deriv WebSocket..."
        )

        new_ws.run_forever(
            ping_interval=20,
            ping_timeout=10,
            reconnect=0,
        )

    except Exception as exc:

        state["connected"] = False
        state["authorized"] = False

        state["last_error"] = str(
            exc
        )

        add_log(
            f"WS THREAD ERROR: {exc}"
        )

    finally:

        with ws_lock:

            if ws is not None:
                ws = None

        state["connected"] = False
        state["authorized"] = False


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
            "error": "App ID is required.",
        }), 400

    if not token:

        return jsonify({
            "ok": False,
            "error": "PAT/API Token is required.",
        }), 400

    if not account_id:

        return jsonify({
            "ok": False,
            "error": "Account ID is required.",
        }), 400

    try:

        add_log(
            "Checking Deriv account..."
        )

        accounts = get_accounts(
            app_id,
            token
        )

        selected = None

        for account in accounts:

            if str(
                account.get(
                    "account_id",
                    ""
                )
            ).strip() == account_id:

                selected = account
                break

        if selected is None:

            raise RuntimeError(
                "Account ID not found in "
                "the Options accounts returned "
                "for this PAT."
            )

        account_status = str(
            selected.get(
                "status",
                ""
            )
        ).lower()

        if account_status and (
            account_status != "active"
        ):

            raise RuntimeError(
                f"Account status is "
                f"{account_status}."
            )

        add_log(
            "Account found: "
            f"{account_id}"
        )

        # Close previous connection
        close_old_ws()

        reset_connection_state()

        APP_ID = app_id
        PAT_TOKEN = token

        state["account_id"] = account_id

        state["account_type"] = (
            selected.get(
                "account_type",
                ""
            )
        )

        # IMPORTANT:
        # Request OTP and immediately connect.
        add_log(
            "Requesting one-time "
            "WebSocket URL..."
        )

        ws_url = get_otp_url(
            app_id,
            token,
            account_id
        )

        add_log(
            "OTP WebSocket URL received."
        )

        ws_thread = threading.Thread(
            target=websocket_thread,
            args=(ws_url,),
            daemon=True,
        )

        ws_thread.start()

        return jsonify({
            "ok": True,
            "message":
                "Connection started. "
                "Waiting for WebSocket authorization.",
        })

    except Exception as exc:

        reset_connection_state()

        state["last_error"] = str(
            exc
        )

        add_log(
            f"CONNECTION FAILED: {exc}"
        )

        return jsonify({
            "ok": False,
            "error": str(exc),
        }), 400


# ============================================================
# FIND BOOM
# ============================================================

@app.post("/api/markets")
def api_markets():

    if not state["connected"]:

        return jsonify({
            "ok": False,
            "error":
                "Connect Deriv first.",
        }), 400

    send_ws({
        "active_symbols": "full",
        "req_id": 10,
    })

    add_log(
        "Searching for BOOM symbols..."
    )

    return jsonify({
        "ok": True
    })


# ============================================================
# SELECT SYMBOL
# ============================================================

@app.post("/api/select-symbol")
def api_select_symbol():

    if not state["connected"]:

        return jsonify({
            "ok": False,
            "error":
                "WebSocket is not connected.",
        }), 400

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
            "ok": False,
            "error":
                "Symbol is required.",
        }), 400

    state["symbol"] = symbol
    state["last_price"] = None
    state["available_contracts"] = []

    # Tick subscription
    send_ws({
        "ticks": symbol,
        "subscribe": 1,
        "req_id": 11,
    })

    # Contract information
    send_ws({
        "contracts_for": symbol,
        "req_id": 12,
    })

    add_log(
        f"Selected market: {symbol}"
    )

    return jsonify({
        "ok": True,
        "symbol": symbol,
    })


# ============================================================
# START
# ============================================================

@app.post("/api/start")
def api_start():

    if not state["authorized"]:

        return jsonify({
            "ok": False,
            "error":
                "Connect and authorize "
                "Deriv first.",
        }), 400

    if not state["symbol"]:

        return jsonify({
            "ok": False,
            "error":
                "Select a BOOM symbol first.",
        }), 400

    state["running"] = True

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
# STATUS
# ============================================================

@app.get("/api/status")
def api_status():

    total = state["total_trades"]

    if total > 0:

        win_rate = (
            state["wins"]
            / total
            * 100
        )

    else:

        win_rate = 0.0

    return jsonify({

        "connected":
            state["connected"],

        "authorized":
            state["authorized"],

        "balance":
            state["balance"],

        "currency":
            state["currency"],

        "account_id":
            state["account_id"],

        "account_type":
            state["account_type"],

        "running":
            state["running"],

        "symbol":
            state["symbol"],

        "last_price":
            state["last_price"],

        "stake":
            state["stake"],

        "duration":
            state["duration"],

        "direction":
            state["direction"],

        "martingale":
            state["martingale"],

        "total_trades":
            state["total_trades"],

        "wins":
            state["wins"],

        "losses":
            state["losses"],

        "profit":
            state["profit"],

        "win_rate":
            win_rate,

        "loss_streak":
            state["loss_streak"],

        "current_trade":
            state["current_trade"],

        "boom_symbols":
            state["boom_symbols"],

        "available_contracts":
            state["available_contracts"],

        "history":
            state["history"],

        "logs":
            state["logs"],

        "last_error":
            state["last_error"],
    })


# ============================================================
# HOME
# ============================================================

@app.route("/")
def index():

    return render_template(
        "index.html"
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
    )
