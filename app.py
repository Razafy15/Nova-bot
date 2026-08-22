import json
import threading
import time

import requests
import websocket

from flask import Flask, jsonify, render_template, request


app = Flask(__name__)

# ============================================================
# BOT STATE
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

    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "profit": 0.0,
    "loss_streak": 0,

    "stake": 0.35,
    "duration": 3,

    "current_trade": None,
    "history": [],
    "logs": [],

    "contracts": [],

    "last_error": "",
}

ws = None
ws_lock = threading.Lock()

APP_ID = ""
PAT_TOKEN = ""


# ============================================================
# LOG
# ============================================================

def add_log(message):

    timestamp = time.strftime("%H:%M:%S")

    state["logs"].insert(
        0,
        {
            "time": timestamp,
            "message": message,
        }
    )

    state["logs"] = state["logs"][:50]

    print(f"[{timestamp}] {message}")


# ============================================================
# WEBSOCKET SEND
# ============================================================

def send_ws(payload):

    global ws

    with ws_lock:

        if ws is None:
            return False

        try:

            ws.send(json.dumps(payload))

            return True

        except Exception as exc:

            state["last_error"] = str(exc)

            add_log(
                f"WebSocket send error: {exc}"
            )

            return False


# ============================================================
# DERIV REST
# ============================================================

def get_accounts(app_id, token):

    url = (
        "https://api.derivws.com"
        "/trading/v1/options/accounts"
    )

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.get(
        url,
        headers=headers,
        timeout=20,
    )

    if not response.ok:

        raise RuntimeError(
            f"Accounts request failed "
            f"HTTP {response.status_code}: "
            f"{response.text}"
        )

    result = response.json()

    return result.get("data", [])


def get_otp_url(
    app_id,
    token,
    account_id
):

    url = (
        "https://api.derivws.com"
        f"/trading/v1/options/accounts/"
        f"{account_id}/otp"
    )

    headers = {
        "Deriv-App-ID": app_id,
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.post(
        url,
        headers=headers,
        timeout=20,
    )

    if not response.ok:

        raise RuntimeError(
            f"OTP request failed "
            f"HTTP {response.status_code}: "
            f"{response.text}"
        )

    result = response.json()

    ws_url = (
        result
        .get("data", {})
        .get("url")
    )

    if not ws_url:

        raise RuntimeError(
            f"No WebSocket URL: {result}"
        )

    return ws_url


# ============================================================
# WEBSOCKET CALLBACKS
# ============================================================

def on_open(socket):

    state["connected"] = True
    state["last_error"] = ""

    add_log(
        "Deriv WebSocket connected."
    )

    send_ws({
        "balance": 1,
        "subscribe": 1,
        "req_id": 1,
    })


def on_message(socket, message):

    try:

        data = json.loads(message)

    except Exception:

        return

    msg_type = data.get(
        "msg_type"
    )


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
            ValueError
        ):

            state["balance"] = 0.0


        state["currency"] = (
            balance_data.get(
                "currency"
            )
            or "USD"
        )

        state["authorized"] = True

        add_log(
            "Account authorized. "
            f"Balance: "
            f"{state['balance']:.2f} "
            f"{state['currency']}"
        )


    # --------------------------------------------------------
    # ACTIVE SYMBOLS
    # --------------------------------------------------------

    elif msg_type == "active_symbols":

        symbols = data.get(
            "active_symbols",
            []
        )

        boom_symbols = []

        for item in symbols:

            symbol = str(
                item.get(
                    "symbol",
                    ""
                )
            )

            display_name = str(
                item.get(
                    "display_name",
                    ""
                )
            )

            text = (
                symbol + " " +
                display_name
            ).upper()

            if "BOOM" in text:

                boom_symbols.append({
                    "symbol": symbol,
                    "display_name":
                        display_name,
                })


        state["contracts"] = (
            boom_symbols
        )

        add_log(
            f"Found {len(boom_symbols)} "
            "BOOM symbols."
        )


    # --------------------------------------------------------
    # TICK
    # --------------------------------------------------------

    elif msg_type == "tick":

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

        except (
            TypeError,
            ValueError
        ):

            pass


    # --------------------------------------------------------
    # CONTRACTS FOR
    # --------------------------------------------------------

    elif msg_type == "contracts_for":

        contracts = (
            data
            .get(
                "contracts_for",
                {}
            )
            .get(
                "available",
                []
            )
        )

        state["contracts"] = contracts

        add_log(
            "Contract types received."
        )


    # --------------------------------------------------------
    # PROPOSAL
    # --------------------------------------------------------

    elif msg_type == "proposal":

        add_log(
            "Proposal received."
        )


    # --------------------------------------------------------
    # BUY
    # --------------------------------------------------------

    elif msg_type == "buy":

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

                "type":
                    "PUT/FALL",

                "stake":
                    state["stake"],

                "status":
                    "OPEN",
            }

            add_log(
                f"Contract opened: "
                f"{contract_id}"
            )

            send_ws({
                "proposal_open_contract": 1,
                "contract_id":
                    contract_id,
                "subscribe": 1,
                "req_id": 7,
            })


    # --------------------------------------------------------
    # OPEN CONTRACT
    # --------------------------------------------------------

    elif msg_type == "proposal_open_contract":

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
                ValueError
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

                    "type":
                        "PUT/FALL",

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


    # --------------------------------------------------------
    # ERROR
    # --------------------------------------------------------

    elif msg_type == "error":

        error = data.get(
            "error",
            {}
        )

        message_text = (
            error.get("message")
            or "Deriv API error"
        )

        state["last_error"] = (
            message_text
        )

        add_log(
            f"API ERROR: "
            f"{message_text}"
        )


def on_error(socket, error):

    state["connected"] = False
    state["authorized"] = False

    state["last_error"] = str(
        error
    )

    add_log(
        f"WebSocket error: {error}"
    )


def on_close(
    socket,
    code,
    message
):

    state["connected"] = False
    state["authorized"] = False

    add_log(
        f"WebSocket closed: "
        f"{code} {message}"
    )


# ============================================================
# WEBSOCKET THREAD
# ============================================================

def start_websocket(ws_url):

    global ws

    ws = websocket.WebSocketApp(
        ws_url,

        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )

    ws.run_forever(
        ping_interval=20,
        ping_timeout=10,
    )


# ============================================================
# CONNECT
# ============================================================

@app.post("/api/connect")
def api_connect():

    global APP_ID
    global PAT_TOKEN

    data = (
        request
        .get_json(
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
                "App ID is required.",
        }), 400


    if not token:

        return jsonify({
            "ok": False,
            "error":
                "PAT is required.",
        }), 400


    if not account_id:

        return jsonify({
            "ok": False,
            "error":
                "Account ID is required.",
        }), 400


    try:

        accounts = get_accounts(
            app_id,
            token
        )

        selected = None

        for account in accounts:

            if (
                account.get(
                    "account_id"
                )
                == account_id
            ):

                selected = account
                break


        if selected is None:

            raise RuntimeError(
                "Account ID was not found "
                "for this PAT."
            )


        ws_url = get_otp_url(
            app_id,
            token,
            account_id
        )


        APP_ID = app_id
        PAT_TOKEN = token


        state["account_id"] = (
            account_id
        )

        state["account_type"] = (
            selected.get(
                "account_type",
                ""
            )
        )

        state["last_error"] = ""


        thread = threading.Thread(
            target=start_websocket,
            args=(ws_url,),
            daemon=True,
        )

        thread.start()


        return jsonify({
            "ok": True,
            "message":
                "Connection started.",
        })


    except Exception as exc:

        state["connected"] = False
        state["authorized"] = False

        state["last_error"] = str(
            exc
        )

        add_log(
            f"Connection failed: {exc}"
        )

        return jsonify({
            "ok": False,
            "error": str(exc),
        }), 400


# ============================================================
# START
# ============================================================

@app.post("/api/start")
def api_start():

    if not state["authorized"]:

        return jsonify({
            "ok": False,
            "error":
                "Connect Deriv first.",
        }), 400


    state["running"] = True

    add_log(
        "Bot STARTED."
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
        "Bot PAUSED."
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
        "Bot STOPPED."
    )

    return jsonify({
        "ok": True
    })


# ============================================================
# REQUEST BOOM SYMBOLS
# ============================================================

@app.post("/api/markets")
def api_markets():

    if not state["authorized"]:

        return jsonify({
            "ok": False,
            "error":
                "Connect Deriv first.",
        }), 400


    send_ws({
        "active_symbols":
            "brief",

        "product_type":
            "basic",

        "req_id":
            10,
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
        request
        .get_json(
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
                "Symbol required.",
        }), 400


    state["symbol"] = symbol


    send_ws({
        "ticks":
            symbol,

        "subscribe":
            1,

        "req_id":
            11,
    })


    send_ws({
        "contracts_for":
            symbol,

        "req_id":
            12,
    })


    add_log(
        f"Selected market: {symbol}"
    )


    return jsonify({
        "ok": True,
        "symbol": symbol,
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

        "stake":
            state["stake"],

        "duration":
            state["duration"],

        "current_trade":
            state["current_trade"],

        "history":
            state["history"],

        "logs":
            state["logs"],

        "contracts":
            state["contracts"],

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
# RUN LOCAL
# ============================================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False,
    )
