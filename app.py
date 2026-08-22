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
ws_lock = threading.Lock()

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


def add_log(message):
    timestamp = time.strftime("%H:%M:%S")

    state["logs"].insert(
        0,
        {
            "time": timestamp,
            "message": str(message)
        }
    )

    state["logs"] = state["logs"][:100]

    print(f"[{timestamp}] {message}", flush=True)


def send_ws(payload):
    global ws

    with ws_lock:
        if ws is None:
            add_log("WebSocket is not initialized.")
            return False

        try:
            ws.send(json.dumps(payload))
            return True

        except Exception as exc:
            state["last_error"] = str(exc)
            add_log(f"WS SEND ERROR: {exc}")
            return False


def api_error(response):
    try:
        data = response.json()
    except Exception:
        return f"HTTP {response.status_code}: {response.text[:500]}"

    errors = data.get("errors")

    if isinstance(errors, list) and errors:
        error = errors[0]

        return (
            f"HTTP {response.status_code} | "
            f"{error.get('code', 'ERROR')} | "
            f"{error.get('message', 'Unknown error')}"
        )

    return f"HTTP {response.status_code}: {json.dumps(data)[:500]}"


# ============================================================
# DERIV REST
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

    return accounts


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

    ws_url = data.get("data", {}).get("url")

    if not ws_url:
        raise RuntimeError(
            "OTP response did not contain data.url"
        )

    return ws_url


# ============================================================
# STRATEGY
# ============================================================

def check_strategy():
    interval = state.get("trade_interval", 5)

    last_time = state.get("last_trade_time", 0)

    current_time = time.time()

    if current_time - last_time >= interval:

        # Tazonina PUT/FALL araka ny bot-nao
        state["direction"] = "PUT"

        return True

    return False


# ============================================================
# TRADING LOOP
# ============================================================

def trading_loop():

    add_log("=== TRADING LOOP STARTED ===")

    loop_count = 0

    while state["running"]:

        loop_count += 1

        try:

            if loop_count % 10 == 0:
                add_log(
                    f"Trading loop alive (cycle {loop_count})"
                )

            # Tsy mbola authorized
            if not state["authorized"]:
                time.sleep(0.5)
                continue

            # Tsy misy symbol
            if not state["symbol"]:
                time.sleep(0.5)
                continue

            # Mbola misy contract misokatra
            if state["current_trade"] is not None:
                time.sleep(0.5)
                continue

            # Tsy mbola tonga ny interval
            if not check_strategy():
                time.sleep(0.5)
                continue

            symbol = state["symbol"]
            direction = state["direction"]
            stake = float(state["stake"])
            duration = int(state["duration"])

            add_log(
                f"Strategy triggered: "
                f"{direction} | "
                f"{symbol} | "
                f"{stake} {state['currency']} | "
                f"{duration}s"
            )

            # ==================================================
            # PROPOSAL
            # ==================================================
            proposal_payload = {
                "proposal": 1,

                "amount": stake,

                "basis": "stake",

                "contract_type": direction,

                "currency": state["currency"],

                "duration": duration,

                "duration_unit": "s",

                # IMPORTANT:
                # API vaovao = underlying_symbol
                "underlying_symbol": symbol,

                "subscribe": 1,

                "req_id": 700,
            }

            # Diovina aloha ny proposal taloha
            state["last_proposal"] = None

            add_log(
                "Sending PROPOSAL..."
            )

            add_log(
                f"Symbol sent: {symbol}"
            )

            add_log(
                f"Contract type: {direction}"
            )

            add_log(
                f"Duration: {duration}s"
            )

            add_log(
                f"Stake: {stake}"
            )

            ok = send_ws(proposal_payload)

            if not ok:
                add_log(
                    "PROPOSAL SEND FAILED."
                )

                time.sleep(2)
                continue

            # ==================================================
            # MIANDRY PROPOSAL RESPONSE
            # ==================================================

            proposal_wait_start = time.time()

            while (
                time.time() - proposal_wait_start < 8
                and state["running"]
            ):

                proposal = state.get("last_proposal")

                if proposal and proposal.get("id"):

                    break

                time.sleep(0.1)

            proposal = state.get("last_proposal")

            if not proposal or not proposal.get("id"):

                add_log(
                    "PROPOSAL TIMEOUT: "
                    "No valid proposal received."
                )

                continue

            proposal_id = proposal.get("id")

            ask_price = proposal.get("ask_price")

            if ask_price is None:

                add_log(
                    "PROPOSAL ERROR: ask_price is missing."
                )

                state["last_proposal"] = None

                continue

            try:
                ask_price = float(ask_price)

            except Exception:

                add_log(
                    f"Invalid ask_price: {ask_price}"
                )

                state["last_proposal"] = None

                continue

            add_log(
                f"PROPOSAL OK: "
                f"id={proposal_id} "
                f"ask_price={ask_price}"
            )

            # ==================================================
            # BUY
            # ==================================================

            buy_payload = {
                "buy": proposal_id,

                "price": ask_price,

                "req_id": 701,
            }

            add_log(
                f"BUY sent: "
                f"proposal={proposal_id} "
                f"price={ask_price}"
            )

            buy_ok = send_ws(buy_payload)

            if not buy_ok:

                add_log(
                    "BUY SEND FAILED."
                )

                state["last_proposal"] = None

                time.sleep(2)

                continue

            # Tsy avela handefa proposal hafa avy hatrany
            state["last_trade_time"] = time.time()

            state["last_proposal"] = None

            # Miandry kely
            time.sleep(1)

        except Exception as e:

            add_log(
                f"TRADING LOOP ERROR: {e}"
            )

            import traceback

            add_log(
                traceback.format_exc()
            )

            time.sleep(2)

    add_log(
        "=== TRADING LOOP STOPPED ==="
    )


# ============================================================
# WEBSOCKET OPEN
# ============================================================

def on_open(socket):

    state["connected"] = True

    state["last_error"] = ""

    add_log(
        "WebSocket connected."
    )

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
# WEBSOCKET MESSAGE
# ============================================================

def on_message(socket, message):

    try:

        data = json.loads(message)

    except Exception:

        add_log(
            "Invalid JSON received."
        )

        return

    msg_type = data.get("msg_type")

    # ========================================================
    # ERROR
    # ========================================================

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
            f"DERIV ERROR: "
            f"{code}: {message_text}"
        )

        # Raha proposal error
        echo_req = data.get("echo_req", {})

        if echo_req.get("proposal") == 1:

            add_log(
                "The error came from PROPOSAL."
            )

            add_log(
                f"Proposal request: "
                f"{json.dumps(echo_req)}"
            )

        return

    # ========================================================
    # BALANCE
    # ========================================================

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
            f"Account authorized. "
            f"Balance: "
            f"{state['balance']:.2f} "
            f"{state['currency']}"
        )

        return

    # ========================================================
    # ACTIVE SYMBOLS
    # ========================================================

    if msg_type == "active_symbols":

        symbols = data.get(
            "active_symbols",
            []
        )

        boom = []

        for item in symbols:

            # API vaovao
            symbol = str(
                item.get(
                    "underlying_symbol",
                    ""
                )
            ).strip()

            display_name = str(
                item.get(
                    "underlying_symbol_name",
                    ""
                )
            ).strip()

            if not symbol:
                continue

            text = (
                symbol
                + " "
                + display_name
            ).upper()

            # BOOM ihany
            if "BOOM" in text:

                boom.append({
                    "symbol": symbol,
                    "display_name":
                        display_name or symbol
                })

        state["boom_symbols"] = boom

        add_log(
            f"BOOM symbols found: "
            f"{len(boom)}"
        )

        for item in boom:

            add_log(
                f"BOOM: "
                f"{item['symbol']} = "
                f"{item['display_name']}"
            )

        return

    # ========================================================
    # TICK
    # ========================================================

    if msg_type == "tick":

        tick = data.get(
            "tick",
            {}
        )

        try:

            price = float(
                tick.get("quote")
            )

            state["last_price"] = price

            state["ticks_buffer"].append(
                price
            )

            if len(
                state["ticks_buffer"]
            ) > 20:

                state[
                    "ticks_buffer"
                ].pop(0)

        except Exception:

            pass

        return

    # ========================================================
    # CONTRACTS FOR
    # ========================================================

    if msg_type == "contracts_for":

        result = data.get(
            "contracts_for",
            {}
        )

        state["available_contracts"] = (
            result.get(
                "available",
                []
            )
        )

        add_log(
            "Contract information received."
        )

        return

    # ========================================================
    # PROPOSAL
    # ========================================================

    if msg_type == "proposal":

        proposal = data.get(
            "proposal",
            {}
        )

        proposal_id = proposal.get(
            "id"
        )

        if proposal_id:

            ask_price = proposal.get(
                "ask_price"
            )

            state["last_proposal"] = {
                "id": proposal_id,
                "ask_price": ask_price
            }

            add_log(
                f"PROPOSAL RECEIVED: "
                f"id={proposal_id}, "
                f"ask_price={ask_price}"
            )

        else:

            add_log(
                "PROPOSAL received "
                "without proposal id."
            )

        return

    # ========================================================
    # BUY
    # ========================================================

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
                "contract_id":
                    contract_id,

                "symbol":
                    state["symbol"],

                "direction":
                    state["direction"],

                "stake":
                    state["stake"],

                "status":
                    "OPEN",
            }

            state["last_trade_time"] = (
                time.time()
            )

            add_log(
                f"CONTRACT OPENED: "
                f"{contract_id}"
            )

            # Monitor contract
            send_ws({
                "proposal_open_contract": 1,

                "contract_id":
                    contract_id,

                "subscribe": 1,

                "req_id": 300,
            })

        else:

            add_log(
                "BUY response received "
                "without contract_id."
            )

        return

    # ========================================================
    # OPEN CONTRACT
    # ========================================================

    if msg_type == "proposal_open_contract":

        contract = data.get(
            "proposal_open_contract",
            {}
        )

        if not (
            contract.get("is_sold")
            or contract.get("is_expired")
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
                "time":
                    time.strftime(
                        "%H:%M:%S"
                    ),

                "symbol":
                    state["symbol_display"]
                    or state["symbol"],

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
# WEBSOCKET ERROR
# ============================================================

def on_error(socket, error):

    state["connected"] = False

    state["authorized"] = False

    state["last_error"] = str(error)

    add_log(
        f"WebSocket error: {error}"
    )


# ============================================================
# WEBSOCKET CLOSE
# ============================================================

def on_close(socket, code, reason):

    state["connected"] = False

    state["authorized"] = False

    add_log(
        f"WebSocket closed: "
        f"{code} {reason}"
    )


# ============================================================
# WEBSOCKET THREAD
# ============================================================

def websocket_thread(ws_url):

    global ws

    try:

        add_log(
            "Opening authenticated WebSocket..."
        )

        ws = websocket.WebSocketApp(
            ws_url,

            on_open=on_open,

            on_message=on_message,

            on_error=on_error,

            on_close=on_close,
        )

        ws.run_forever(
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


# ============================================================
# FLASK ROUTES
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
            "error":
                "App ID is required."
        }), 400

    if not token:

        return jsonify({
            "ok": False,
            "error":
                "PAT/API Token is required."
        }), 400

    if not account_id:

        return jsonify({
            "ok": False,
            "error":
                "Account ID is required."
        }), 400

    try:

        add_log(
            "========== CONNECT =========="
        )

        accounts = get_accounts(
            app_id,
            token
        )

        selected = None

        for account in accounts:

            if (
                str(
                    account.get(
                        "account_id",
                        ""
                    )
                ).strip()
                == account_id
            ):

                selected = account

                break

        if selected is None:

            raise RuntimeError(
                "Account ID was not found "
                "for this PAT."
            )

        state["account_id"] = (
            account_id
        )

        state["account_type"] = (
            selected.get(
                "account_type",
                ""
            )
        )

        ws_url = get_otp_url(
            app_id,
            token,
            account_id
        )

        APP_ID = app_id

        PAT_TOKEN = token

        if (
            ws_thread
            and ws_thread.is_alive()
        ):

            add_log(
                "Closing old "
                "WebSocket thread..."
            )

            if ws:

                try:
                    ws.close()

                except Exception:
                    pass

            time.sleep(1)

        ws_thread = threading.Thread(
            target=websocket_thread,
            args=(ws_url,),
            daemon=True
        )

        ws_thread.start()

        add_log(
            "OTP obtained. "
            "WebSocket connection started."
        )

        return jsonify({
            "ok": True,
            "message":
                "Connection started."
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
# MARKETS
# ============================================================

@app.post("/api/markets")
def api_markets():

    if not state["connected"]:

        return jsonify({
            "ok": False,
            "error":
                "Connect Deriv first."
        }), 400

    send_ws({
        "active_symbols": "full",
        "req_id": 500
    })

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
            "error":
                "Symbol is required."
        }), 400

    if not state["connected"]:

        return jsonify({
            "ok": False,
            "error":
                "Connect Deriv first."
        }), 400

    # Arovanana ny R_ satria tsy BOOM
    # BOOM symbol tokony ahitana BOOM
    if "BOOM" not in (
        symbol + " " + display_name
    ).upper():

        return jsonify({
            "ok": False,
            "error":
                "This is not a BOOM symbol."
        }), 400

    state["symbol"] = symbol

    state["symbol_display"] = (
        display_name
    )

    state["available_contracts"] = []

    state["ticks_buffer"] = []

    state["last_proposal"] = None

    # Tick subscription
    send_ws({
        "ticks": symbol,
        "subscribe": 1,
        "req_id": 600
    })

    # Available contracts
    send_ws({
        "contracts_for": symbol,
        "req_id": 601
    })

    add_log(
        f"Selected BOOM symbol: "
        f"{symbol} "
        f"({display_name})"
    )

    return jsonify({
        "ok": True,
        "symbol": symbol,
        "display_name":
            display_name
    })


# ============================================================
# START
# ============================================================

@app.post("/api/start")
def api_start():

    add_log(
        "START button pressed."
    )

    if not state["authorized"]:

        add_log(
            "ERROR: Not authorized yet."
        )

        return jsonify({
            "ok": False,
            "error":
                "Connect and authorize "
                "Deriv first."
        }), 400

    if not state["symbol"]:

        add_log(
            "ERROR: No BOOM symbol selected."
        )

        return jsonify({
            "ok": False,
            "error":
                "Select a BOOM symbol first."
        }), 400

    state["running"] = True

    state["last_trade_time"] = 0

    if (
        not hasattr(
            app,
            "trading_thread"
        )
        or not app.trading_thread.is_alive()
    ):

        add_log(
            "Creating new trading thread..."
        )

        app.trading_thread = threading.Thread(
            target=trading_loop,
            daemon=True
        )

        app.trading_thread.start()

        add_log(
            "Trading thread started."
        )

    else:

        add_log(
            "Trading thread already running."
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
# UPDATE INTERVAL
# ============================================================

@app.post("/api/update-interval")
def api_update_interval():

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    interval = int(
        data.get(
            "interval",
            5
        )
    )

    if interval < 1:

        return jsonify({
            "ok": False,
            "error":
                "Interval must be at least 1 second."
        }), 400

    state["trade_interval"] = interval

    add_log(
        f"Trade interval updated "
        f"to {interval} seconds."
    )

    return jsonify({
        "ok": True,
        "interval": interval
    })


# ============================================================
# UPDATE DURATION
# ============================================================

@app.post("/api/update-duration")
def api_update_duration():

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    duration = int(
        data.get(
            "duration",
            3
        )
    )

    if duration < 1:

        return jsonify({
            "ok": False,
            "error":
                "Duration must be at least 1 second."
        }), 400

    state["duration"] = duration

    add_log(
        f"Duration updated "
        f"to {duration} seconds."
    )

    return jsonify({
        "ok": True,
        "duration": duration
    })


# ============================================================
# STATUS
# ============================================================

@app.get("/api/status")
def api_status():

    total = state["total_trades"]

    win_rate = (
        state["wins"]
        / total
        * 100
        if total
        else 0.0
    )

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
# MAIN
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
